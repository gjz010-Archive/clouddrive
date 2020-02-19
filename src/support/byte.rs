use super::{CloudProvider, CloudProviderExt};
use std::io::Write;
use std::ops::Range;
use std::cmp::{max, min};
use std::slice::SliceIndex;
use async_trait::async_trait;
/// Allows writing to any cloud provider in a manner that is not block-aligned.
/// This is done by read-and-write-back. Note that without proper caching this will cost two RTLs.
pub struct ByteGranularityProvider<T: CloudProvider>{
    provider: T
}
impl<T: CloudProvider> ByteGranularityProvider<T>{
    pub fn new(provider: T)->Self{
        ByteGranularityProvider{provider}
    }
    fn underlying_block_size(&self)->usize{
        self.provider.block_size()
    }
    fn underlying_block_range(&self, offset: usize, size: usize)->Vec<(usize, Range<usize>, Range<usize>)>{
        self.provider.block_range(offset, size)
    }

}
#[async_trait]
impl<T: CloudProvider> CloudProvider for ByteGranularityProvider<T> {
    fn total_size(&self) -> usize {
        self.provider.total_size()
    }

    async unsafe fn unsafe_write(&mut self, offset: usize, buf: &[u8], write_through: bool) -> Result<(), std::io::Error> {
        let block_size=self.underlying_block_size();
        let range=self.underlying_block_range(offset, buf.len());
        let mut buffer=self.provider.create_block_buffer();
        for (block_id, range_block, range_local) in range.iter(){
            if range_local.end-range_local.start == block_size{
                // Writes through.
                self.provider.unsafe_write_block(*block_id, &buf[Range::clone(range_local)], write_through).await?;
            }else{
                // Read a block then modify and then write back.
                self.provider.unsafe_read_block(*block_id, &mut buffer).await?;
                std::ptr::copy_nonoverlapping(buf.as_ptr().add(range_local.start) as *const u8, buffer.as_mut_ptr().add(range_block.start) as *mut u8, range_local.end-range_local.start);
                self.provider.unsafe_write_block(*block_id, &mut buffer, write_through).await?;
            }

        }
        Ok(())
    }

    async unsafe fn unsafe_read(&mut self, offset: usize, buf: &mut [u8]) -> Result<(), std::io::Error> {
        let block_size=self.underlying_block_size();
        let range=self.underlying_block_range(offset, buf.len());
        let mut buffer=self.provider.create_block_buffer();
        for (block_id, range_block, range_local) in range.iter(){
            if range_local.end-range_local.start == block_size{
                // Read through.
                self.provider.unsafe_read_block(*block_id, &mut buf[Range::clone(range_local)]).await?;
            }else{
                // Read a block then slice out.
                self.provider.unsafe_read_block(*block_id, &mut buffer).await?;
                std::ptr::copy_nonoverlapping(buffer.as_ptr().add(range_block.start) as *const u8, buf.as_mut_ptr().add(range_local.start) as *mut u8,range_local.end-range_local.start);
            }

        }
        Ok(())
    }

    async fn flush(&mut self) -> Result<(), std::io::Error> {
        self.provider.flush().await
    }

    fn block_size(&self) -> usize {
        1
    }
}