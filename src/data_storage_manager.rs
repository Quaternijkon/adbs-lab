use crate::define::PageId;
use std::fs::{File, OpenOptions};
use std::io::{Seek, SeekFrom, Read, Write};
use std::sync::Mutex;

pub struct DSMgr {
    file: Mutex<File>,
    num_pages: Mutex<PageId>,
    write_num: Mutex<PageId>,
}

impl DSMgr {
    pub fn new(filename: &str) -> std::io::Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(filename)?;
        
        let metadata = file.metadata()?;
        let file_size = metadata.len();
        let num_pages = (file_size / crate::define::PAGE_SIZE as u64) as PageId;
        
        Ok(Self {
            file: Mutex::new(file),
            num_pages: Mutex::new(num_pages),
            write_num: Mutex::new(0),
        })
    }

    pub fn open_file(filename: &str) -> std::io::Result<Self> {
        Self::new(filename)
    }

    pub fn close_file(&self) -> std::io::Result<()> {
        let mut file = self.file.lock().unwrap();
        file.flush()
    }

    pub fn new_page(&self) -> std::io::Result<PageId> {
        let mut num_pages = self.num_pages.lock().unwrap();
        let page_id = *num_pages;
        *num_pages += 1;
        // Initialize the new page with zeros
        let mut file = self.file.lock().unwrap();
        file.seek(SeekFrom::End(0))?;
        file.write_all(&vec![0u8; crate::define::PAGE_SIZE])?;
        Ok(page_id)
    }

    pub fn read_page(&self, page_id: PageId, data: &mut [u8]) -> std::io::Result<()> {
        let mut file = self.file.lock().unwrap();
        file.seek(SeekFrom::Start(page_id as u64 * crate::define::PAGE_SIZE as u64))?;
        file.read_exact(data)?;
        Ok(())
    }

    pub fn write_page(&self, page_id: PageId, data: &[u8]) -> std::io::Result<()> {
        let mut file = self.file.lock().unwrap();
        file.seek(SeekFrom::Start(page_id as u64 * crate::define::PAGE_SIZE as u64))?;
        file.write_all(data)?;
        let mut write_num = self.write_num.lock().unwrap();
        *write_num += 1;
        Ok(())
    }

    pub fn get_num_pages(&self) -> PageId {
        let num_pages = self.num_pages.lock().unwrap();
        *num_pages
    }

    pub fn get_write_num(&self) -> PageId {
        let write_num = self.write_num.lock().unwrap();
        *write_num
    }
}
