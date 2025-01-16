use crate::define::FrameId;
use crate::replacer::Replacer;
use std::sync::Mutex;


pub struct ClockReplacer {
    frames: Mutex<Vec<(FrameId, bool)>>, 
    pointer: Mutex<usize>,
}

impl ClockReplacer {
    pub fn new(frame_num: usize) -> Self {
        Self {
            frames: Mutex::new(Vec::with_capacity(frame_num)),
            pointer: Mutex::new(0),
        }
    }
}

impl Replacer for ClockReplacer {
    fn victim(&self) -> Option<FrameId> {
        let mut frames = self.frames.lock().unwrap();
        let mut pointer = self.pointer.lock().unwrap();

        if frames.is_empty() {
            return None;
        }

        let frames_len = frames.len();
        for _ in 0..frames_len {
            if *pointer >= frames.len() {
                *pointer = 0;
            }
            let (_frame_id, second_chance) = &mut frames[*pointer];
            if *second_chance {
                *second_chance = false;
                *pointer = (*pointer + 1) % frames.len();
            } else {
                let victim = frames.remove(*pointer);
                return Some(victim.0);
            }
        }
        None
    }

    fn insert(&self, frame_id: FrameId) {
        let mut frames = self.frames.lock().unwrap();
        frames.push((frame_id, true));
    }

    fn remove(&self, frame_id: FrameId) {
        let mut frames = self.frames.lock().unwrap();
        if let Some(pos) = frames.iter().position(|&(id, _)| id == frame_id) {
            frames.remove(pos);
        }
    }

    fn size(&self) -> usize {
        let frames = self.frames.lock().unwrap();
        frames.len()
    }

    fn print(&self) {
        let frames = self.frames.lock().unwrap();
        print!("Clock Replacer Frames: [");
        for (frame_id, second_chance) in frames.iter() {
            print!("({}, {}), ", frame_id, second_chance);
        }
        println!("]");
    }
}
