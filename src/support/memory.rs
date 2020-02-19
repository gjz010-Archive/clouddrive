use tokio::sync::RwLock;
use async_trait::async_trait;
use std::mem::MaybeUninit;

pub struct MemoryProvider{
    size: usize,
    content: Vec<u8>
}

impl MemoryProvider {
    pub fn new(size: usize)->MemoryProvider{
        let data={
            let mut d=Vec::with_capacity(size);
            unsafe {d.set_len(size);}
            d
        };
        /*
        let mut data=Vec::new();
        data.resize(size, unsafe {MaybeUninit::uninit().assume_init()});
        */
        MemoryProvider{
            size,
            content: data
        }
    }
}
#[async_trait]
impl super::CloudProvider for MemoryProvider {
    fn total_size(&self) -> usize {
        self.size
    }

    // Since this is unsafe-write we don't consider border check.
    async unsafe fn unsafe_write(&mut self, offset: usize, buf: &[u8], write_through: bool) -> std::io::Result<()> {
        let content=&mut self.content;
        std::ptr::copy_nonoverlapping(buf.as_ptr() as *const u8, content.as_mut_ptr().add(offset), buf.len());
        Ok(())
    }

    async unsafe fn unsafe_read(&mut self, offset: usize, buf: &mut [u8]) -> std::io::Result<()> {
        let content=&mut self.content;
        std::ptr::copy_nonoverlapping(content.as_ptr().add(offset), buf.as_mut_ptr() as *mut u8, buf.len());
        Ok(())
    }

    async fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }

    fn block_size(&self) -> usize {
        crate::nbd::PREFERRED_BLOCK_SIZE
    }
}