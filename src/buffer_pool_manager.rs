use crate::define::{PageId, FrameId, PAGE_SIZE};
use crate::page::Page;
use crate::replacer::Replacer;
use crate::lru_replacer::LRUReplacer;
use crate::clock_replacer::ClockReplacer;
use crate::data_storage_manager::DSMgr;
use std::collections::HashMap;
use std::sync::{Mutex, Arc};
use std::sync::atomic::{AtomicI32, Ordering};

pub enum ReplacePolicyType {
    LRU,
    Clock,
}


pub struct BufferPoolManager {
    disk_manager: Arc<DSMgr>,
    frame_num: usize,
    pages: Vec<Mutex<Page>>,
    free_list: Mutex<Vec<FrameId>>,
    page_table: Mutex<HashMap<PageId, FrameId>>,
    num_io: AtomicI32,
    num_hits: AtomicI32,
    replacer: Arc<dyn Replacer>,
}

impl BufferPoolManager {
    pub fn new(filename: &str, policy: ReplacePolicyType, frame_num: usize) -> std::io::Result<Self> {
        let disk_manager = Arc::new(DSMgr::open_file(filename)?);
        
        let mut pages = Vec::with_capacity(frame_num);
        for _ in 0..frame_num {
            pages.push(Mutex::new(Page::new()));
        }
        
        let free_list = (0..frame_num as FrameId).collect();
        
        let replacer: Arc<dyn Replacer> = match policy {
            ReplacePolicyType::LRU => Arc::new(LRUReplacer::new(frame_num)),
            ReplacePolicyType::Clock => Arc::new(ClockReplacer::new(frame_num)),
        };

        Ok(Self {
            disk_manager,
            frame_num,
            pages,
            free_list: Mutex::new(free_list),
            page_table: Mutex::new(HashMap::new()),
            num_io: AtomicI32::new(0),
            num_hits: AtomicI32::new(0),
            replacer,
        })
    }

    pub fn fix_page(&self, page_id: PageId, is_dirty: bool) -> std::io::Result<FrameId> {
        
        {
            let page_table = self.page_table.lock().unwrap();
            if let Some(&frame_id) = page_table.get(&page_id) {
                self.num_hits.fetch_add(1, Ordering::SeqCst);
                
                {
                    let mut page = self.pages[frame_id as usize].lock().unwrap();
                    if is_dirty {
                        page.set_dirty(true);
                    }
                }
                
                return Ok(frame_id);
            }
        }

        
        
        let frame_id = {
            let mut free_list = self.free_list.lock().unwrap();
            if let Some(frame_id) = free_list.pop() {
                frame_id
            } else {
                
                
                drop(free_list);
                match self.replacer.victim() {
                    Some(victim) => victim,
                    None => return Err(std::io::Error::new(std::io::ErrorKind::Other, "No available frame")),
                }
            }
        };

        
        {
            let mut page = self.pages[frame_id as usize].lock().unwrap();
            let old_page_id = page.get_page_id();
            if old_page_id != -1 {
                if page.is_dirty() {
                    self.disk_manager.write_page(old_page_id, page.get_data())?;
                }
                
                {
                    let mut page_table = self.page_table.lock().unwrap();
                    page_table.remove(&old_page_id);
                }
            }

            
            let mut data = [0u8; PAGE_SIZE];
            self.disk_manager.read_page(page_id, &mut data)?;
            page.set_page_id(page_id);
            page.set_dirty(is_dirty);
            page.set_data(&data);

            
            self.num_io.fetch_add(1, Ordering::SeqCst);
        }

        
        {
            let mut page_table = self.page_table.lock().unwrap();
            page_table.insert(page_id, frame_id);
        }

        Ok(frame_id)
    }

    pub fn fix_new_page(&self, page_id: &mut PageId) -> std::io::Result<FrameId> {
        let new_page_id = self.disk_manager.new_page()?;
        *page_id = new_page_id;
        self.fix_page(new_page_id, false)
    }

    pub fn unfix_page(&self, _page_id: PageId) {
        
        
    }

    pub fn get_io_num(&self) -> i32 {
        self.num_io.load(Ordering::SeqCst)
    }

    pub fn get_hit_num(&self) -> i32 {
        self.num_hits.load(Ordering::SeqCst)
    }

    pub fn get_num_pages(&self) -> PageId {
        self.disk_manager.get_num_pages()
    }

    pub fn print_page_table(&self) {
        let page_table = self.page_table.lock().unwrap();
        println!("Page Table: {:?}", *page_table);
    }

    pub fn print_replacer(&self) {
        self.replacer.print();
    }
}
