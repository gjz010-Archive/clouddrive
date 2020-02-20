use reqwest::*;
use tokio::prelude::*;
use async_trait::async_trait;
use std::ops::Range;
use crate::support::{CloudProvider, CloudProviderExt};
use std::io::ErrorKind;
use bytes::Buf;
use std::fmt;

pub const BLOCK_SIZE:usize=1*1024;
pub struct SeafileProvider {
    total_size: usize,
    http: Client,
    token: String,
    library_path: String
}
#[derive(Debug)]
pub enum SeafileError{
    AuthError,
    NoLibraryError,
    IOError(Box<dyn std::error::Error+Send+Sync>),
    BadTotalSizeError,
    BadResponseError(u16)
}
impl fmt::Display for SeafileError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        // Use `self.number` to refer to each positional data point.
        write!(f, "{:?}", self)
    }
}
impl std::error::Error for SeafileError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        // Generic error, underlying cause isn't tracked.
        None
    }
}
impl From<SeafileError> for std::io::Error{
    fn from(e: SeafileError) -> Self {
        std::io::Error::new(ErrorKind::Other, e)
    }
}
const SEAFILE_API_BASE: &str="https://cloud.tsinghua.edu.cn/api2/";
const SEAFILE_AUTH_PING: &str="https://cloud.tsinghua.edu.cn/api2/ping/";
const SEAFILE_LIBRARY_BASE:&str="https://cloud.tsinghua.edu.cn/api2/repos/{}/";
const SEAFILE_LIBRARY_FILE:&str="https://cloud.tsinghua.edu.cn/api2/repos/{}/file/?p=/{}.block";
const SEAFILE_LIBRARY_UPLOAD_LINK:&str="https://cloud.tsinghua.edu.cn/api2/repos/{}/upload-link/";
fn seafile_library_base(library: &str)->String{
    format!("https://cloud.tsinghua.edu.cn/api2/repos/{}/", library)
}
fn seafile_library_file(library: &str, file: usize)->String{
    format!("https://cloud.tsinghua.edu.cn/api2/repos/{}/file/?p=/{}.block", library, file)
}
fn seafile_library_upload_link(library: &str)->String{
    format!("https://cloud.tsinghua.edu.cn/api2/repos/{}/upload-link/", library)
}
pub type Result<T>=std::result::Result<T, SeafileError>;
impl SeafileProvider{
    pub async fn connect(token: &str, library: &str, total_size: usize)->Result<Self>{
        if total_size % BLOCK_SIZE !=0{
            return Err(SeafileError::BadTotalSizeError);
        }
        let mut headers = header::HeaderMap::new();
        headers.insert(header::AUTHORIZATION, header::HeaderValue::from_str(token).unwrap());
        let mut client_builder=ClientBuilder::new().user_agent("CloudDrive Seafile Provider").default_headers(headers).tcp_nodelay();
        if let Ok(url)=std::env::var("https_proxy"){
            client_builder=client_builder.proxy(Proxy::https(&url).unwrap());
        }

        if let Ok(url)=std::env::var("http_proxy"){
            client_builder=client_builder.proxy(Proxy::http(&url).unwrap());
        }
        let mut seafile=SeafileProvider {
            total_size,
            http: client_builder.build().unwrap(),
            token: String::from(token),
            library_path: String::from(library)
        };
        seafile.ping().await?;
        Ok(seafile)
    }
    async fn ping(&mut self)->Result<()>{
        match self.http.get(SEAFILE_AUTH_PING).send().await{
            Ok(response)=>{
                if response.status()==reqwest::StatusCode::OK{
                    match self.http.get(&seafile_library_base(&self.library_path)).send().await {
                        Ok(response)=>{
                            if response.status()==reqwest::StatusCode::OK{
                                Ok(())
                            }else{
                                Err(SeafileError::NoLibraryError)
                            }
                        }
                        Err(error)=>Err(SeafileError::IOError(Box::new(error)))
                    }
                }else{
                    Err(SeafileError::AuthError)
                }
            }
            Err(error)=>Err(SeafileError::IOError(Box::new(error)))
        }
    }
    async fn get_block(&mut self, block_id: usize, buf: &mut [u8])->Result<()>{
        match self.http.get(&seafile_library_file(&self.library_path, block_id)).send().await{
            Ok(response)=>{
                if response.status()==reqwest::StatusCode::OK{
                    let text:String=response.text().await.unwrap();
                    let url=&text[1..text.len()-1]; //remove quotes.
                    match self.http.get(url).send().await{
                        Ok(response)=>{
                            match response.bytes().await{
                                Ok(mut buffer)=>{
                                    buffer.copy_to_slice(&mut buf[0..buffer.remaining()]);
                                    Ok(())
                                }
                                Err(error)=>Err(SeafileError::IOError(Box::new(error)))
                            }
                        }
                        Err(error)=>Err(SeafileError::IOError(Box::new(error)))
                    }
                }else if response.status()==reqwest::StatusCode::NOT_FOUND{
                    // considered as uninitialized chunks.
                    Ok(())
                }else {
                    Err(SeafileError::BadResponseError(response.status().as_u16()))
                }
            }
            Err(error)=>Err(SeafileError::IOError(Box::new(error)))
        }
    }
    async fn put_block(&mut self, block_id: usize, buf: &[u8])->Result<()>{
        match self.http.get(&seafile_library_upload_link(&self.library_path)).send().await{
            Ok(response)=>{
                if response.status()==reqwest::StatusCode::OK{
                    let text:String=response.text().await.unwrap();
                    let url=&text[1..text.len()-1]; //remove quotes.
                    let copied_bytes=buf.to_vec();
                    let part=multipart::Part::bytes(copied_bytes).file_name(format!("{}.block", block_id));
                    let form = reqwest::multipart::Form::new().text("parent_dir","/").text("replace","1").part("file",part);
                    match self.http.post(url).multipart(form).send().await{
                        Ok(response)=>{
                            Ok(())
                        }
                        Err(error)=>Err(SeafileError::IOError(Box::new(error)))
                    }
                }else if response.status()==reqwest::StatusCode::NOT_FOUND{
                    // considered as uninitialized chunks.
                    Ok(())
                }else {
                    Err(SeafileError::BadResponseError(response.status().as_u16()))
                }
            }
            Err(error)=>Err(SeafileError::IOError(Box::new(error)))
        }
    }
}
#[async_trait]
impl CloudProvider for SeafileProvider {
    fn total_size(&self) -> usize {
        self.total_size
    }

    async unsafe fn unsafe_write(&mut self, offset: usize, buf: &[u8], _write_through: bool) -> std::io::Result<()> {
        let range=self.block_range(offset, buf.len());
        let block_size=self.block_size();
        println!("Write {} {}", offset, buf.len());
        for (block_id, _range_block, range_local) in range.iter(){
            let mut retries=0;
            'retry:loop{
                match self.put_block(*block_id, &buf[Range::clone(range_local)]).await{
                    Ok(())=>{break'retry;}
                    Err(err)=>{
                        retries+=1;
                        eprintln!("Seafile put_block error {:?}, starting retry #{}...", err, retries);
                    }
                }
            }
        }
        println!("Write {} {} done", offset, buf.len());
        Ok(())
    }

    async unsafe fn unsafe_read(&mut self, offset: usize, buf: &mut [u8]) -> std::io::Result<()> {
        println!("Read {} {}", offset, buf.len());
        let range=self.block_range(offset, buf.len());
        let block_size=self.block_size();
        for (block_id, _range_block, range_local) in range.iter(){
            let mut retries=0;
            'retry:loop {
                match self.get_block(*block_id, &mut buf[Range::clone(range_local)]).await{
                    Ok(())=>{break'retry;}
                    Err(err)=>{
                        retries+=1;
                        eprintln!("Seafile put_block error {:?}, starting retry #{}...", err, retries);
                    }
                }
            }
        }
        println!("Read {} {} done", offset, buf.len());
        Ok(())
    }

    async fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }

    fn block_size(&self) -> usize {
        BLOCK_SIZE
    }
}