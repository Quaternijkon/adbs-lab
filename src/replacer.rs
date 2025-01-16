use crate::define::FrameId;

pub trait Replacer: Send + Sync {
    fn victim(&self) -> Option<FrameId>;
    fn insert(&self, frame_id: FrameId);
    fn remove(&self, frame_id: FrameId);
    fn size(&self) -> usize;
    fn print(&self);
}
