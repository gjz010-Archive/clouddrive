use tokio::prelude::*;
use std::error::Error;
use std::*;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::Mutex;
use std::mem::MaybeUninit;
use crate::support::{CloudProvider, SharedProvider, bound_and_align_check, MutexProvider};
use std::sync::Arc;
use std::collections::BTreeMap;

const NBDMAGIC:u64=0x4e42444d41474943;
const IHAVEOPT:u64=0x49484156454F5054;
const HANDSHAKE_FLAGS:u16=1; // Does not support NBD_FLAG_NO_ZEROES
const REPLY_OPT:u64=0x3e889045565a9;

pub const PREFERRED_BLOCK_SIZE:usize=4096;

const NBD_OPT_EXPORT_NAME:u32=1;
const NBD_OPT_ABORT:u32=2;
const NBD_OPT_LIST:u32=3;

const NBD_OPT_INFO:u32=6;
const NBD_OPT_GO:u32=7;

const NBD_REP_ACK:u32=1;
const NBD_REP_SERVER:u32=1;
const NBD_REP_INFO:u32=3;

const NBD_REP_ERR_PREFIX:u32=2147483648;
const NBD_REP_ERR_UNSUP:u32=NBD_REP_ERR_PREFIX+1;
const NBD_REP_ERR_INVALID:u32=NBD_REP_ERR_PREFIX+3;


const NBD_FLAG_HAS_FLAGS:u16=1<<0;
const NBD_FLAG_READ_ONLY:u16=1<<1;
const NBD_FLAG_SEND_FLUSH:u16=1<<2;
const NBD_FLAG_SEND_FUA:u16=1<<3;
const NBD_FLAG_ROTATIONAL:u16=1<<4;
const NBD_FLAG_SEND_TRIM:u16=1<<5;
const NBD_FLAG_SEND_WRITE_ZEROES:u16=1<<6;
const NBD_FLAG_SEND_DF:u16=1<<7;
const NBD_FLAG_CAN_MULTI_CONN:u16=1<<8;
const NBD_FLAG_SEND_RESIZE:u16=1<<9;
const NBD_FLAG_SEND_CACHE:u16=1<<10;
const NBD_FLAG_SEND_FAST_ZERO:u16=1<<11;

const NBD_REQUEST_MAGIC:u32=0x25609513;

const NBD_CMD_FLAG_FUA:u16=1<<0;
const NBD_CMD_FLAG_NO_HOLE:u16=1<<1;
const NBD_CMD_FLAG_DF:u16=1<<2;
const NBD_CMD_FLAG_REQ_ONE:u16=1<<3;
const NBD_CMD_FLAG_FAST_ZERO:u16=1<<4;


const NBD_CMD_READ:u16=0;
const NBD_CMD_WRITE:u16=1;
const NBD_CMD_DISC:u16=2;
const NBD_CMD_FLUSH:u16=3;
const NBD_CMD_TRIM:u16=4;
const NBD_CMD_CACHE:u16=5;
const NBD_CMD_WRITE_ZEROES:u16=6;
const NBD_CMD_BLOCK_STATUS:u16=7;
const NBD_CMD_RESIZE:u16=8;

const NBD_SIMPLE_REPLY_MAGIC:u32=0x67446698;

const NBD_EPERM:u32=1;
const NBD_EIO:u32=5;
const NBD_ENOMEM:u32=12;
const NBD_EINVAL:u32=22;
const NBD_ENOSPC:u32=28;
const NBD_EOVERFLOW:u32=75;
const NBD_ENOTSUP:u32=95;
const NBD_ESHUTDOWN:u32=108;

#[derive(Debug, Clone)]
enum NBDError {
    ClientFlagsError,
    ClientOptionsError,
    BadExportError,
    Abort,
    RequestMagicError
}

// Generation of an error is completely separate from how it is displayed.
// There's no need to be concerned about cluttering complex logic with the display style.
//
// Note that we don't store any extra info about the errors. This means we can't state
// which string failed to parse without modifying our types to carry that information.
impl fmt::Display for NBDError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "NBDError::{}", &self)
    }
}

// This is important for other errors to wrap this one.
impl Error for NBDError {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        // Generic error, underlying cause isn't tracked.
        None
    }
}
struct NBDClient<T: AsyncRead+AsyncWrite+Unpin>{
    stream: T
}

struct ClientOption{
    pub option: u32,
    pub data: Vec<u8>
}
struct OptionReply{
    pub option: u32,
    pub reply_type: u32,
    pub data: Vec<u8>,
}
struct ExportItem{
    pub size: u64,
    pub transmission_flags: u16
}

async fn read_nbd_client_option<T: AsyncRead+Unpin>(stream: &mut T)->Result<ClientOption, Box<dyn Error>>{
    let magic=stream.read_u64().await?;
    if magic!=IHAVEOPT {
        return Err(NBDError::ClientOptionsError)?;
    }
    let option=stream.read_u32().await?;
    let data_size=stream.read_u32().await?;
    let mut data:Vec<u8>=Vec::new();
    data.resize(data_size as usize, unsafe {MaybeUninit::uninit().assume_init()});
    stream.read_exact(&mut data).await?;
    Ok(ClientOption{
        option, data
    })
}
async fn write_nbd_option_reply<T: AsyncWrite + Unpin>(stream: &mut T, reply: OptionReply)->Result<(), Box<dyn Error>>{
    stream.write_u64(REPLY_OPT).await?;
    stream.write_u32(reply.option).await?;
    stream.write_u32(reply.reply_type).await?;
    stream.write_u32(reply.data.len() as u32).await?;
    stream.write_all(&reply.data).await?;
    Ok(())
}
async fn write_nbd_export_item<T: AsyncWrite + Unpin>(stream: &mut T, reply: ExportItem)->Result<(), Box<dyn Error>>{
    stream.write_u64(reply.size).await?;
    stream.write_u16(reply.transmission_flags).await?;
    //let zero:[u8;124]=[0;124];
    //stream.write_all(&zero).await?;
    Ok(())
}


pub async fn handshake<T: AsyncRead+AsyncWrite+Unpin>(stream: &mut T, exports: &BTreeMap<String, Arc<Mutex<Box<dyn CloudProvider>>>>)->Result<Arc<Mutex<Box<dyn CloudProvider>>>, Box<dyn Error>>{
    println!("Incoming handshake...");
    stream.write_u64(NBDMAGIC).await?;
    stream.write_u64(IHAVEOPT).await?;
    stream.write_u16(HANDSHAKE_FLAGS).await?;
    stream.flush().await?;
    let client_flags=stream.read_u32().await?;
    if client_flags!=1 {
        return Err(NBDError::ClientFlagsError)?;
    }
    loop {
        let option=read_nbd_client_option(stream).await?;
        match option.option{
            NBD_OPT_EXPORT_NAME=>{
                println!("NBD_OPT_EXPORT_NAME");
                let name=String::from_utf8(Vec::clone(&option.data))?;
                if let Some(provider)=exports.get(&name){
                    write_nbd_export_item(stream, ExportItem {size: provider.lock().await.total_size() as u64, transmission_flags: NBD_FLAG_HAS_FLAGS|NBD_FLAG_SEND_FLUSH|NBD_FLAG_SEND_FUA}).await?;
                    stream.flush().await?;
                    return Ok(Arc::clone(provider));
                }else{
                    return Err(NBDError::BadExportError)?;
                }

            }
            NBD_OPT_ABORT=>{
                println!("NBD_ABORT");
                write_nbd_option_reply(stream, OptionReply{option: NBD_OPT_ABORT, reply_type: NBD_REP_ACK, data: Vec::new()}).await?;
                stream.flush().await?;
                return Err(NBDError::Abort)?;
            }
            _=>{
                println!("Unknown option: {}", option.option);
                write_nbd_option_reply(stream, OptionReply {option: option.option, reply_type: NBD_REP_ERR_UNSUP, data: Vec::new()}).await?;
                stream.flush().await?;
            }
        }

    }
}


struct TransmissionRequest {
    flags: u16,
    cmdtype: u16,
    handle: u64,
    offset: u64,
    length: u32,
    data: Option<Vec<u8>>
}
struct TransmissionSimpleResponse{
    error: u32,
    handle: u64,
    data: Option<Vec<u8>>
}
impl TransmissionSimpleResponse{
    pub async fn write_to<T: AsyncWrite+Unpin>(self, stream: &mut T)->Result<(), Box<dyn Error>>{
        stream.write_u32(NBD_SIMPLE_REPLY_MAGIC).await?;
        stream.write_u32(self.error).await?;
        stream.write_u64(self.handle).await?;
        if let Some(data)=self.data{
            stream.write_all(&data).await?;
        }
        Ok(())
    }
}
async fn read_transmission_request<T: AsyncRead+Unpin>(stream: &mut T)->Result<TransmissionRequest, Box<dyn Error>>{
    let magic=stream.read_u32().await?;
    if magic!=NBD_REQUEST_MAGIC {
        return Err(NBDError::RequestMagicError)?;
    }else{
        let flags=stream.read_u16().await?;
        let cmdtype=stream.read_u16().await?;
        let handle=stream.read_u64().await?;
        let offset=stream.read_u64().await?;
        let length=stream.read_u32().await?;
        let data=if cmdtype==NBD_CMD_WRITE{
            /// TODO: Dangerous when there is not maximum block size negotiation!
            let mut data:Vec<u8>=Vec::new();
            data.resize(length as usize, unsafe {MaybeUninit::uninit().assume_init()});
            stream.read_exact(&mut data).await?;
            Some(data)
        }else{
            None
        };
        Ok(TransmissionRequest{flags, cmdtype, handle, offset, length, data})
    }
}
pub async fn handle_packet<T: AsyncRead+AsyncWrite+Unpin>(stream: &mut T, provider: &mut Box<dyn CloudProvider>)->Result<(), Box<dyn Error>>{
    'mainloop:loop {
        let req=read_transmission_request(stream).await?;
        let block_size=provider.block_size();
        let total_size=provider.total_size();
        match req.cmdtype{
            NBD_CMD_READ=>{
                //println!("NBD_CMD_READ received. offset={} length={}", req.offset, req.length);
                if !bound_and_align_check(block_size, total_size, req.offset as usize, req.length as usize){
                    TransmissionSimpleResponse{error: NBD_EINVAL, handle: req.handle, data: None}.write_to(stream).await?;
                }else{
                    if req.length==0{
                        TransmissionSimpleResponse{error: NBD_EINVAL, handle: req.handle, data: None}.write_to(stream).await?;
                    }else{
                        /// TODO: Dangerous when there is not maximum block size negotiation!
                        let mut data:Vec<u8>=Vec::new();
                        data.resize(req.length as usize, unsafe {MaybeUninit::uninit().assume_init()});
                        match provider.read(req.offset as usize, &mut data).await {
                            Ok(())=>{
                                TransmissionSimpleResponse{error: 0, handle: req.handle, data: Some(data)}.write_to(stream).await?;
                            }
                            Err(err)=>{
                                eprintln!("NBD_CMD_READ error: {:?}", err);
                                TransmissionSimpleResponse{error: NBD_EIO, handle: req.handle, data: None}.write_to(stream).await?;
                            }
                        }

                    }

                }
            }
            NBD_CMD_WRITE=>{
                //println!("NBD_CMD_write received. offset={} length={}", req.offset, req.length);
                if (req.offset as usize) >= total_size || (req.offset as usize)+(req.length as usize) > total_size{
                    TransmissionSimpleResponse{error: NBD_ENOSPC, handle: req.handle, data: None}.write_to(stream).await?;
                }else if (req.offset as usize) % block_size!=0 || (req.length as usize) % block_size!=0{
                    TransmissionSimpleResponse{error: NBD_EINVAL, handle: req.handle, data: None}.write_to(stream).await?;
                }else{
                    let fua_write_through=(req.flags|NBD_CMD_FLAG_FUA) >0;
                    if req.length==0{
                        TransmissionSimpleResponse{error: NBD_EINVAL, handle: req.handle, data: None}.write_to(stream).await?;
                    }else{
                        match provider.write(req.offset as usize, req.data.as_ref().unwrap(), fua_write_through).await{
                            Ok(())=>{
                                TransmissionSimpleResponse{error: 0, handle: req.handle, data: None}.write_to(stream).await?;
                            }
                            Err(err)=>{
                                eprintln!("NBD_CMD_WRITE error: {:?}", err);
                                TransmissionSimpleResponse{error: NBD_EIO, handle: req.handle, data: None}.write_to(stream).await?;
                            }
                        }
                    }
                }
            }
            NBD_CMD_DISC=>{
                println!("NBD_CMD_DISC received.");
                TransmissionSimpleResponse{error: 0, handle: req.handle, data: None}.write_to(stream).await?;
                stream.flush().await?;
                break 'mainloop Ok(());
            }
            NBD_CMD_FLUSH=>{
                println!("NBD_CMD_FLUSH received.");
                match provider.flush().await{
                    Ok(())=>{
                        TransmissionSimpleResponse{error: 0, handle: req.handle, data: None}.write_to(stream).await?;
                    }
                    Err(err)=>{
                        eprintln!("NBD_CMD_FLUSH error: {:?}", err);
                        TransmissionSimpleResponse{error: NBD_EIO, handle: req.handle, data: None}.write_to(stream).await?;
                    }
                }
            }
            _=>{
                println!("Unknown command: {}", req.cmdtype);
                TransmissionSimpleResponse{error: NBD_EINVAL, handle: req.handle, data: None}.write_to(stream).await?;
            }
        }
        stream.flush().await?;
    }
}

