use super::{CloudProvider, CloudProviderExt};
use std::ops::{Range};
use std::pin::Pin;
use lru::LruCache;
use std::sync::Arc;
use std::cmp::{max, min};
use async_trait::async_trait;
use std::slice::SliceIndex;
pub struct LRUItem{
    pub data: Vec<u8>,
    pub dirty: bool
}
/// Wrapper for any cloud provider, with local LRU cache.
pub struct LRUProvider<T: CloudProvider>{
    provider: T,
    cache: LruCache<usize, LRUItem>,
    capacity: usize
}


impl<T: CloudProvider> LRUProvider<T>{
    pub fn new(provider:T, capacity: usize)->LRUProvider<T>{
        LRUProvider{
            provider,
            capacity,
            cache: LruCache::unbounded()
        }
    }

}
#[async_trait]
impl<T: CloudProvider> CloudProvider for LRUProvider<T>{
    fn total_size(&self) -> usize {
        self.provider.total_size()
    }

    async unsafe fn unsafe_write(&mut self, offset: usize, buf: &[u8], write_through: bool) -> std::io::Result<()> {
        let range=self.block_range(offset, buf.len());
        let block_size=self.block_size();
        for (block_id, _range_block, range_local) in range.iter(){
            if let Some(block)=self.cache.get_mut(block_id){
                // Copy to cache and mark as dirty.
                std::ptr::copy_nonoverlapping(buf.as_ptr().add(range_local.start) as *const u8, block.data.as_mut_ptr() as *mut u8, block_size);
                if write_through{
                    self.provider.unsafe_write_block(*block_id, &block.data, true).await?;
                    block.dirty=false;
                }else{
                    block.dirty=true;
                }
            }else{
                // Cache miss.
                let mut buffer=self.provider.create_block_buffer();
                self.provider.unsafe_read_block(*block_id, &mut buffer).await?;
                std::ptr::copy_nonoverlapping(buf.as_ptr().add(range_local.start), buffer.as_mut_ptr() as *mut u8, block_size);
                // insert into cache.
                if self.cache.len()==self.capacity {
                    let (evicted_id, evicted_item)=self.cache.pop_lru().unwrap(); // assert capacity>0.
                    if evicted_item.dirty {
                        self.provider.unsafe_write_block(evicted_id, &evicted_item.data, false).await?;
                    }
                }
                let mut lruitem=LRUItem {data: buffer, dirty: true};
                if write_through{
                    self.provider.unsafe_write_block(*block_id, &lruitem.data, true).await?;
                    lruitem.dirty=false;
                }
                self.cache.put(*block_id, lruitem);
            }

        }
        Ok(())
    }

    async unsafe fn unsafe_read(&mut self, offset: usize, buf: &mut [u8]) -> std::io::Result<()> {
        let range=self.block_range(offset, buf.len());

        for (block_id, _range_block, range_local) in range.iter(){
            if let Some(block)=self.cache.get(block_id){
                // Copy from cache.
                std::ptr::copy_nonoverlapping( block.data.as_ptr() as *const u8, buf.as_mut_ptr().add(range_local.start) as *mut u8,self.block_size());
            }else{
                // Cache miss.
                let mut buffer=self.provider.create_block_buffer();
                self.provider.unsafe_read_block(*block_id, &mut buffer).await?;
                std::ptr::copy_nonoverlapping(buffer.as_ptr() as *const u8, buf.as_mut_ptr().add(range_local.start) as *mut u8, self.block_size());
                // insert into cache.
                if self.cache.len()==self.capacity {
                    let (evicted_id, evicted_item)=self.cache.pop_lru().unwrap(); // assert capacity>0.
                    if evicted_item.dirty {
                        self.provider.unsafe_write_block(evicted_id, &evicted_item.data, false).await?;
                    }
                }
                let mut lruitem=LRUItem {data: buffer, dirty: false};
                self.cache.put(*block_id, lruitem);
            }

        }
        Ok(())
    }


    async fn flush(&mut self) -> std::io::Result<()> {
        unsafe {
            for (block_id, lruitem) in self.cache.iter_mut() {
                if lruitem.dirty {
                    self.provider.unsafe_write_block(*block_id, &lruitem.data, true).await?;
                    lruitem.dirty = false;
                }
            }
        }
        Ok(())
    }

    fn block_size(&self) -> usize {
        self.provider.block_size()
    }
}