use tokio::prelude::*;
use std::error::Error;
use std::*;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::Mutex;
use std::mem::MaybeUninit;
use crate::support::{CloudProvider, SharedProvider};
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

#[derive(Debug, Clone)]
enum NBDError {
    ClientFlagsError,
    ClientOptionsError,
    BadExportError,
    Abort
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
pub struct ExportDescriptor{
    name: String,
    size: u64
}
impl ExportDescriptor {
    pub fn new(name: &str, size: u64)->Self{
        ExportDescriptor{
            name: String::from(name),
            size
        }
    }
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
    stream.write_all(&[0;124]).await?;
    Ok(())
}


pub async fn handshake<T: AsyncRead+AsyncWrite+Unpin>(stream: &mut T, exports: BTreeMap<String, Arc<dyn SharedProvider>>)->Result<(), Box<dyn Error>>{
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
                let name=String::from_utf8(Vec::clone(&option.data))?;
                if let Some(provider)=exports.get(&name){
                    write_nbd_export_item(stream, ExportItem {size: provider.total_size() as u64, transmission_flags: NBD_FLAG_HAS_FLAGS|NBD_FLAG_SEND_FLUSH|NBD_FLAG_SEND_FUA|NBD_FLAG_SEND_CACHE}).await?;
                    stream.flush().await?;
                    break;
                }else{
                    return Err(NBDError::BadExportError)?;
                }

            }
            NBD_OPT_ABORT=>{
                write_nbd_option_reply(stream, OptionReply{option: NBD_OPT_ABORT, reply_type: NBD_REP_ACK, data: Vec::new()}).await?;
                stream.flush().await?;
                return Err(NBDError::Abort)?;
            }
            _=>{
                write_nbd_option_reply(stream, OptionReply {option: option.option, reply_type: NBD_REP_ERR_UNSUP, data: Vec::new()}).await?;
                stream.flush().await?;
            }
        }

    }
    Ok(())
}

pub async fn handle_packet<T: AsyncRead+AsyncWrite>(stream: &mut T)->Result<(), Box<dyn Error>>{
    Ok(())
}

