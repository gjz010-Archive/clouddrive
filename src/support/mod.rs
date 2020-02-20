use std::io::ErrorKind;
use std::cmp::{max, min};
use std::ops::Range;
use tokio::sync::Mutex;
use async_trait::async_trait;
mod lru;
mod memory;
mod byte;
pub mod seafile;
use std::ops::DerefMut;

pub use self::byte::ByteGranularityProvider;
pub use self::lru::LRUProvider;
pub use self::memory::MemoryProvider;
pub use self::seafile::SeafileProvider;
pub fn bound_and_align_check(block_size: usize, total_size: usize, offset: usize, size: usize)->bool{
    if offset%block_size!=0 || size%block_size!=0 {
        return false;
    }else if offset+size > total_size{
        return false;
    } else if offset>=total_size{
        return false;
    }
    true
}
#[async_trait]
pub trait CloudProvider: Send+Sync {
    fn total_size(&self)->usize;
    async unsafe fn unsafe_write(&mut self, offset: usize, buf: &[u8], write_through: bool)->std::io::Result<()>;
    async unsafe fn unsafe_read(&mut self, offset: usize, buf: &mut [u8])->std::io::Result<()>;
    async fn write(&mut self, offset: usize, buf: &[u8], write_through: bool)->std::io::Result<()>{

        if !bound_and_align_check(self.block_size(), self.total_size(), offset, buf.len()) {
            return Err(ErrorKind::InvalidInput)?;
        }else{
            unsafe {self.unsafe_write(offset, buf, write_through).await}
        }
    }
    async fn read(&mut self, offset: usize, buf: &mut [u8])->std::io::Result<()>{
        if !bound_and_align_check(self.block_size(), self.total_size(), offset, buf.len()) {
            return Err(ErrorKind::InvalidInput)?;
        }else{
            unsafe {self.unsafe_read(offset, buf).await}
        }
    }
    async fn flush(&mut self)->std::io::Result<()>;
    fn block_size(&self)->usize;
}

#[async_trait]
pub trait CloudProviderExt{
    fn block_index(&self, offset: usize)->usize;
    fn block_range(&self, offset: usize, size: usize)->Vec<(usize, Range<usize>, Range<usize>)>;
    fn create_block_buffer(&self)->Vec<u8>;
    async unsafe fn unsafe_read_block(&mut self, block_offset: usize, buf: &mut [u8])->std::io::Result<()>;
    async unsafe fn unsafe_write_block(&mut self, block_offset: usize, buf: &[u8], write_through: bool)->std::io::Result<()>;
}
#[async_trait]
impl<T: CloudProvider+Send> CloudProviderExt for T {

    fn block_index(&self, offset: usize)->usize{
        offset/self.block_size()
    }
    fn block_range(&self, offset: usize, size: usize)->Vec<(usize, Range<usize>, Range<usize>)>{
        let block_size=self.block_size();
        let first_block=self.block_index(offset);
        let last_block=self.block_index(offset+size-1);
        let first_block_offset = (offset%block_size);
        (first_block..=last_block).map(|block_id| {
            let global_lower=max(offset, block_id*block_size);
            let global_upper=min(size+offset, (block_id+1)*block_size);
            let affected_length=global_upper-global_lower;
            let block_lower=global_lower%block_size;
            let block_upper=block_lower+affected_length;
            let range_block=(block_lower..block_upper);
            let range_local=(global_lower-offset .. global_upper-offset);
            (block_id, range_block, range_local)
        }).collect()

    }
    fn create_block_buffer(&self)->Vec<u8>{
        unsafe {
            let mut buffer=Vec::with_capacity(self.block_size());
            buffer.set_len(self.block_size());
            buffer
        }
    }
    async unsafe fn unsafe_read_block(&mut self, block_offset: usize, buf: &mut [u8])->std::io::Result<()>{
        self.unsafe_read(block_offset*self.block_size(), buf).await
    }
    async unsafe fn unsafe_write_block(&mut self, block_offset: usize, buf: &[u8], write_through: bool)->std::io::Result<()>{
        self.unsafe_write(block_offset*self.block_size(), buf, write_through).await
    }
}
#[async_trait]
pub trait SharedProvider: Send+Sync{
    fn total_size(&self)->usize;
    async fn write(&self, offset: usize, buf: &[u8], write_through: bool)->std::io::Result<()>;
    async fn read(&self, offset: usize, buf: &mut [u8])->std::io::Result<()>;
    async fn flush(&self)->std::io::Result<()>;
    fn block_size(&self)->usize;
}
pub struct MutexProvider<T: CloudProvider+Sized>{
    provider: Mutex<Box<T>>,
    block_size: usize,
    total_size: usize
}

impl<T: CloudProvider+Send+Sync> MutexProvider<T>{
    pub fn new(provider: T)->Self{
        let (block_size, total_size)=(provider.block_size(), provider.total_size());
        MutexProvider{
            provider: Mutex::new(Box::new(provider)),
            block_size,
            total_size
        }
    }
    pub fn mutex(&self)->&Mutex<Box<T>>{
        &self.provider
    }
}
#[async_trait]
impl<T: CloudProvider + Send+Sync+Sized> SharedProvider for MutexProvider<T>{
    fn total_size(&self)->usize{
        self.total_size
    }
    async fn write(&self, offset: usize, buf: &[u8], write_through: bool)->std::io::Result<()>{
        let mut lock=self.provider.lock().await;
        lock.write(offset, buf, write_through).await
    }
    async fn read(&self, offset: usize, buf: &mut [u8])->std::io::Result<()>{
        let mut lock=self.provider.lock().await;
        lock.read(offset, buf).await
    }
    async fn flush(&self)->std::io::Result<()>{
        let mut lock=self.provider.lock().await;
        lock.flush().await
    }
    fn block_size(&self)->usize{
        self.block_size
    }

}