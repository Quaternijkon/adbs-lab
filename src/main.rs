mod define;
mod page;
mod replacer;
mod lru_replacer;
mod clock_replacer;
mod data_storage_manager;
mod buffer_pool_manager;

use clap::{Arg, ArgAction, Command};
use buffer_pool_manager::{BufferPoolManager, ReplacePolicyType};
use std::thread;
use std::sync::Arc;
use std::io::{BufReader, BufRead};
use std::time::Instant;
use std::fs::{File, OpenOptions};
use std::io::{Write, Seek, SeekFrom};
use std::path::Path;

const PAGE_SIZE: usize = 4096; 
const INITIAL_PAGES: usize = 50000; 

fn main() -> std::io::Result<()> {
    
    let matches = Command::new("Storage and Buffer Manager")
        .version("1.0")
        .author("Your Name")
        .about("Implements Storage and Buffer Manager in Rust")
        .arg(
            Arg::new("lru")
                .short('l')
                .long("lru")
                .help("Use LRU replacer")
                .action(ArgAction::SetTrue),
        )
        .arg(
            Arg::new("clock")
                .short('c')
                .long("clock")
                .help("Use Clock replacer")
                .action(ArgAction::SetTrue),
        )
        .arg(
            Arg::new("multi")
                .short('m')
                .long("multi")
                .help("Use multi-threading")
                .action(ArgAction::SetTrue),
        )
        .arg(
            Arg::new("threads")
                .short('t')
                .long("threads")
                .help("Number of threads")
                .default_value("10")
                .value_parser(clap::value_parser!(usize)),
        )
        .arg(
            Arg::new("FILE")
                .help("Input trace file")
                .required(true)
                .index(1),
        )
        .get_matches();

    
    let policy = if matches.get_flag("lru") {
        ReplacePolicyType::LRU
    } else {
        
        ReplacePolicyType::Clock
    };

    
    let multi = matches.get_flag("multi");

    
    let thread_num: usize = *matches
        .get_one::<usize>("threads")
        .unwrap_or(&10);

    
    let filename = matches
        .get_one::<String>("FILE")
        .expect("FILE argument is required.");

    
    let db_filename = "test.dbf";
    if !Path::new(db_filename).exists() {
        println!("Creating and initializing {}", db_filename);
        let mut db_file = OpenOptions::new()
            .create(true)
            .write(true)
            .read(true)
            .open(db_filename)?;

        
        let buffer = vec![0u8; PAGE_SIZE];
        for _ in 0..INITIAL_PAGES {
            db_file.write_all(&buffer)?;
        }
        db_file.sync_all()?; 
    } else {
        println!("{} already exists.", db_filename);
    }

    
    let bmgr = Arc::new(BufferPoolManager::new(db_filename, policy, 1024)?);

    
    let start_time = Instant::now();

    if multi {
        
        let bmgr_clone = Arc::clone(&bmgr);

        
        let filename_for_threads = filename.clone(); 

        let threads: Vec<_> = (0..thread_num)
            .map(|_| {
                let bmgr = Arc::clone(&bmgr_clone);
                let fname = filename_for_threads.clone(); 
                thread::spawn(move || {
                    if let Ok(file) = std::fs::File::open(&fname) {
                        let reader = BufReader::new(file);
                        for line in reader.lines() {
                            if let Ok(l) = line {
                                let parts: Vec<&str> = l.split(',').collect();
                                if parts.len() != 2 {
                                    continue;
                                }
                                let is_dirty: bool = parts[0].parse::<i32>().unwrap_or(0) != 0;
                                let page_id: i32 = parts[1].parse().unwrap_or(-1);
                                if page_id < 0 {
                                    continue;
                                }
                                
                                let _ = bmgr.fix_page(page_id, is_dirty);
                                bmgr.unfix_page(page_id);
                            }
                        }
                    }
                })
            })
            .collect();

        
        for handle in threads {
            handle.join().expect("Thread panicked");
        }
    } else {
        
        if let Ok(file) = std::fs::File::open(filename) {
            let reader = BufReader::new(file);
            for line in reader.lines() {
                if let Ok(l) = line {
                    let parts: Vec<&str> = l.split(',').collect();
                    if parts.len() != 2 {
                        continue;
                    }
                    let is_dirty: bool = parts[0].parse::<i32>().unwrap_or(0) != 0;
                    let page_id: i32 = parts[1].parse().unwrap_or(-1);
                    if page_id < 0 {
                        continue;
                    }
                    let _ = bmgr.fix_page(page_id, is_dirty);
                    bmgr.unfix_page(page_id);
                }
            }
        } else {
            eprintln!("Error: file {} doesn't exist", filename);
            return Ok(());
        }
    }

    
    let duration = start_time.elapsed();

    
    println!("Hit number: {}", bmgr.get_hit_num());

    
    
    let total_requests = if multi {
        500000 * thread_num as i32
    } else {
        500000
    };

    println!(
        "Hit rate: {:.2}%",
        bmgr.get_hit_num() as f64 * 100.0 / total_requests as f64
    );
    println!("IO number: {}", bmgr.get_io_num());
    println!("Time taken: {:.2?}", duration);

    Ok(())
}
