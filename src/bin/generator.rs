use clap::Parser;
use rand::Rng;
use std::fs::{create_dir_all, File};
use std::io::{BufWriter, Write};
use std::path::Path;


#[derive(Parser, Debug)]
#[command(author, version, about = "生成满足 Zipf 分布的数据文件", long_about = None)]
struct Args {
    
    #[arg(short = 'n', long = "num-files")]
    num_files: usize,

    
    #[arg(short = 'm', long = "num-requests")]
    num_requests: usize,

    
    #[arg(short = 'p', long = "total-pages")]
    total_pages: usize,

    
    #[arg(short = 'z', long = "zipf-param")]
    zipf_param: f64,

    
    #[arg(short = 'r', long = "read-prob")]
    read_prob: f64,
}


struct Zipf {
    cdf: Vec<f64>,
    total_pages: usize,
}

impl Zipf {
    
    fn new(s: f64, n: usize) -> Self {
        let mut harmonic_sum = 0.0;
        let mut probabilities = Vec::with_capacity(n);
        for k in 1..=n {
            harmonic_sum += 1.0 / (k as f64).powf(s);
            probabilities.push(1.0 / (k as f64).powf(s));
        }

        
        let mut cdf = Vec::with_capacity(n);
        let mut cumulative = 0.0;
        for p in probabilities {
            cumulative += p / harmonic_sum;
            cdf.push(cumulative);
        }

        
        if let Some(last) = cdf.last_mut() {
            *last = 1.0;
        }

        Zipf {
            cdf,
            total_pages: n,
        }
    }

    
    fn sample(&self, rng: &mut impl Rng) -> usize {
        let u: f64 = rng.gen();
        match self.cdf.binary_search_by(|probe| probe.partial_cmp(&u).unwrap()) {
            Ok(idx) => idx,
            Err(idx) => idx,
        }
        .min(self.total_pages - 1)
    }
}

fn main() -> std::io::Result<()> {
    
    let args = Args::parse();

    
    if args.total_pages == 0 {
        eprintln!("页面总数必须大于0");
        std::process::exit(1);
    }
    if args.zipf_param <= 0.0 {
        eprintln!("Zipf 分布参数必须大于0");
        std::process::exit(1);
    }
    if !(0.0..=1.0).contains(&args.read_prob) {
        eprintln!("读操作概率必须在0到1之间");
        std::process::exit(1);
    }

    
    let data_dir = Path::new("./data");
    create_dir_all(data_dir)?;

    
    let zipf = Zipf::new(args.zipf_param, args.total_pages);

    
    let mut rng = rand::thread_rng();

    
    for file_idx in 1..=args.num_files {
        
        let filename = format!(
            "zipf{:.2}-{:.2}-{}-data{}.txt",
            args.zipf_param, args.read_prob, args.total_pages, file_idx
        );
        let filepath = data_dir.join(filename);

        
        let file = File::create(filepath)?;
        let mut writer = BufWriter::new(file);

        for _ in 0..args.num_requests {
            
            let op: u8 = if rng.gen::<f64>() < args.read_prob { 0 } else { 1 };

            
            let page = zipf.sample(&mut rng);

            
            writeln!(writer, "{} {}", op, page)?;
        }
    }

    println!("数据文件已生成到 ./data 目录下。");
    Ok(())
}
