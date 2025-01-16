

use crate::define::{PageId, PAGE_SIZE};
use std::sync::atomic::{AtomicI32, Ordering};

pub struct Page {
    page_id: PageId,
    is_dirty: bool,
    data: [u8; PAGE_SIZE],
    pin_count: AtomicI32,
}

impl Page {
    pub fn new() -> Self {
        Self {
            page_id: -1,
            is_dirty: false,
            data: [0; PAGE_SIZE],
            pin_count: AtomicI32::new(0),
        }
    }

    pub fn with_page_id(page_id: PageId) -> Self {
        Self {
            page_id,
            is_dirty: false,
            data: [0; PAGE_SIZE],
            pin_count: AtomicI32::new(0),
        }
    }

    pub fn get_page_id(&self) -> PageId {
        self.page_id
    }

    pub fn set_page_id(&mut self, page_id: PageId) {
        self.page_id = page_id;
    }

    pub fn get_data(&self) -> &[u8; PAGE_SIZE] {
        &self.data
    }

    
    pub fn set_data(&mut self, data: &[u8; PAGE_SIZE]) {
        self.data.copy_from_slice(data);
    }

    pub fn is_dirty(&self) -> bool {
        self.is_dirty
    }

    pub fn set_dirty(&mut self, is_dirty: bool) {
        self.is_dirty = is_dirty;
    }

    pub fn get_pin_count(&self) -> i32 {
        self.pin_count.load(Ordering::SeqCst)
    }

    pub fn inc_pin_count(&self) {
        self.pin_count.fetch_add(1, Ordering::SeqCst);
    }

    pub fn dec_pin_count(&self) {
        self.pin_count.fetch_sub(1, Ordering::SeqCst);
    }
}
