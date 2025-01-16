use crate::define::FrameId;
use crate::replacer::Replacer;
use std::collections::HashSet;
use std::sync::Mutex;


pub struct LRUReplacer {
    inner: Mutex<LruInner>,
    max_size: usize,
}

struct LruInner {
    list: Vec<FrameId>, 
    set: HashSet<FrameId>,
}

impl LRUReplacer {
    pub fn new(max_size: usize) -> Self {
        Self {
            inner: Mutex::new(LruInner {
                list: Vec::new(),
                set: HashSet::new(),
            }),
            max_size,
        }
    }
}

impl Replacer for LRUReplacer {
    fn victim(&self) -> Option<FrameId> {
        let mut inner = self.inner.lock().unwrap();
        if inner.list.is_empty() {
            None
        } else {
            
            let victim = inner.list.remove(0);
            inner.set.remove(&victim);
            Some(victim)
        }
    }

    fn insert(&self, frame_id: FrameId) {
        let mut inner = self.inner.lock().unwrap();
        if inner.set.contains(&frame_id) {
            
            inner.list.retain(|&x| x != frame_id);
            inner.list.push(frame_id);
        } else {
            if inner.list.len() == self.max_size {
                let victim = inner.list.remove(0);
                inner.set.remove(&victim);
            }
            inner.list.push(frame_id);
            inner.set.insert(frame_id);
        }
    }

    fn remove(&self, frame_id: FrameId) {
        let mut inner = self.inner.lock().unwrap();
        if inner.set.remove(&frame_id) {
            inner.list.retain(|&x| x != frame_id);
        }
    }

    fn size(&self) -> usize {
        let inner = self.inner.lock().unwrap();
        inner.list.len()
    }

    fn print(&self) {
        let inner = self.inner.lock().unwrap();
        print!("LRU Replacer List: [");
        for frame_id in inner.list.iter() {
            print!("{}, ", frame_id);
        }
        println!("]");
    }
}
