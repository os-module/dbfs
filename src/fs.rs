use alloc::string::{String, ToString};
use alloc::sync::Arc;
use alloc::{format, vec};
use alloc::vec::Vec;
use core::cmp::{min};
use core::fmt::{Debug, Display, Formatter};
use fat32_trait::{DirectoryLike, FileLike};
use jammdb::{Data, DB, FileOpenOptions, Mmap};
use logger::info;




pub struct FileSystem {
    db: Arc<SafeDb>,
}

impl FileSystem {
    pub fn init() -> Self {
        let db = DB::open::<_,FileOpenOptions,Mmap>("jammdb").unwrap();
        Self { db: Arc::new(SafeDb(db)) }
    }
    pub fn root(&self) -> Arc<DirEntry> {
        // 检查根目录是否存在
        let tx = self.db.0.tx(true).unwrap();
        let _root = tx.get_or_create_bucket("root").unwrap();
        tx.commit().unwrap();
        Arc::new(DirEntry::new(self.db.clone(), "root".to_string()))
    }
}

pub struct File {
    db: Arc<SafeDb>,
    name: String,
    dir: Arc<DirEntry>,
}

pub struct SafeDb(DB);
unsafe impl Sync for SafeDb {}
unsafe impl Send for SafeDb {}

impl Debug for File{
    fn fmt(&self, f: &mut Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("File")
            .field("name", &self.name)
            .field("dir", &self.dir)
            .finish()
    }
}


pub struct DirEntry {
    db: Arc<SafeDb>,
    path: String,
}

impl Debug for DirEntry{
    fn fmt(&self, f: &mut Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("DirEntry")
            .field("path", &self.path)
            .finish()
    }
}

impl File {
    pub fn new(db: Arc<SafeDb>, name: &str, dir: Arc<DirEntry>) -> Self {
        Self {
            db,
            name: name.to_string(),
            dir,
        }
    }
}

impl DirEntry {
    pub fn new(db: Arc<SafeDb>, path: String) -> Self {
        Self { db, path }
    }
}



impl FileLike for File {
    type Error = Error;

    fn read(&self, offset: u32, size: u32) -> Result<Vec<u8>, Self::Error> {
        let tx = self.db.0.tx(false)?;
        let bucket = tx.get_bucket(self.dir.path.as_str())?;

        let bucket = bucket.get_bucket(self.name.as_str())?;
        // where is the offset?
        let data = bucket.get("data").unwrap();
        let data = data.kv().value().to_vec();

        info!("read data len:{}",data.len());
        // read data
        if (data.len() as u32) < offset {
            Ok(vec![])
        } else {
            let end = min(offset + size, data.len() as u32);
            Ok(data[offset as usize..end as usize].to_vec())
        }
    }

    fn write(&self, offset: u32, w_data: &[u8]) -> Result<u32, Self::Error> {
        let tx = self.db.0.tx(true)?;
        let bucket = tx.get_bucket(self.dir.path.as_str())?;
        let bucket = bucket.get_bucket(self.name.as_str())?;
        let data = bucket.get("data").unwrap();
        let mut data = data.kv().value().to_vec();
        if (data.len() as u32) < offset {
            data.resize(offset as usize, 0);
            for _ in data.len()..offset as usize{
                data.push(0);
            }
        }

        info!("data len:{}",data.len());
        data.extend_from_slice(&w_data);

        info!("extended data len: {}",data.len());
        bucket.put("data", data).unwrap();
        tx.commit().unwrap();
        Ok(w_data.len() as u32)
    }

    fn clear(&self) {
        let tx = self.db.0.tx(true).unwrap();
        let bucket = tx.get_bucket(self.dir.path.as_str()).unwrap();
        let bucket = bucket.get_bucket(self.name.as_str()).unwrap();
        bucket.put("data", vec![]).unwrap();
        tx.commit().unwrap();
    }

    fn size(&self) -> u32 {
        let tx = self.db.0.tx(false).unwrap();
        let bucket = tx.get_bucket(self.dir.path.as_str()).unwrap();
        let bucket = bucket.get_bucket(self.name.as_str()).unwrap();
        let data = bucket.get("data").unwrap();
        let data = data.kv().value().len();
        data as u32
    }
}

impl DirectoryLike for DirEntry {
    type Error = Error;

    fn create_dir(&self, name: &str) -> Result<(), Self::Error> {
        let tx = self.db.0.tx(true)?;
        let bucket = tx.get_bucket(self.path.as_str())?;
        bucket.create_bucket(name).unwrap();
        tx.commit().unwrap();
        Ok(())
    }

    fn create_file(&self, name: &str) -> Result<(), Self::Error> {
        {
            let tx = self.db.0.tx(true)?;
            let bucket = tx.get_bucket(self.path.as_str())?;
            let bucket = bucket.create_bucket(name).unwrap();
            bucket.put("data", vec![]).unwrap();
            tx.commit().unwrap();
        }
        Ok(())
    }

    fn delete_dir(&self, name: &str) -> Result<(), Self::Error> {
        let tx = self.db.0.tx(true)?;
        let bucket = tx.get_bucket(self.path.as_str())?;
        bucket.delete_bucket(name).unwrap();
        tx.commit().unwrap();
        Ok(())
    }

    fn delete_file(&self, name: &str) -> Result<(), Self::Error> {
        let tx = self.db.0.tx(true)?;
        let bucket = tx.get_bucket(self.path.as_str())?;
        bucket.delete_bucket(name)?;
        tx.commit().unwrap();
        Ok(())
    }

    fn cd(&self, name: &str) -> Result<Arc<dyn DirectoryLike<Error = Self::Error>>, Self::Error> {
        let tx = self.db.0.tx(false)?;
        let bucket = tx.get_bucket(self.path.as_str())?;
        let _bucket = bucket.get_bucket(name)?;
        Ok(Arc::new(DirEntry::new(self.db.clone(), format!("{}/{}", self.path, name))))
    }

    fn open(&self, name: &str) -> Result<Arc<dyn FileLike<Error = Self::Error>>, Self::Error> {
        let tx = self.db.0.tx(false)?;
        let bucket = tx.get_bucket(self.path.as_str())?;
        let _bucket = bucket.get_bucket(name)?;
        let dir = Arc::new(DirEntry::new(self.db.clone(), self.path.clone()));
        Ok(Arc::new(File::new(self.db.clone(), name, dir)))
    }

    fn list(&self) -> Result<Vec<String>, Self::Error> {
        let tx = self.db.0.tx(false)?;
        let bucket = tx.get_bucket(self.path.as_str())?;
        let mut list = Vec::new();
        bucket.cursor().into_iter().for_each(|data|{
            let name = match &*data {
                Data::Bucket(b)=>b.name(),
                Data::KeyValue(kv) => kv.key(),
            };
            list.push(String::from_utf8(name.to_vec()).unwrap());
        });
        Ok(list)
    }

    fn rename_file(&self, old_name: &str, new_name: &str) -> Result<(), Self::Error> {
        if old_name==new_name{
            return Ok(())
        }
        let tx = self.db.0.tx(true)?;
        let r_bucket = tx.get_bucket(self.path.as_str())?;
        let bucket = r_bucket.get_bucket(old_name).unwrap();
        let data = bucket.get("data").unwrap();
        let data = data.kv().value().to_vec();
        r_bucket.delete_bucket(old_name).unwrap();
        r_bucket.create_bucket(new_name).unwrap();
        let bucket = r_bucket.get_bucket(new_name).unwrap();
        bucket.put("data", data).unwrap();
        tx.commit().unwrap();
        Ok(())
    }

    fn rename_dir(&self, old_name: &str, new_name: &str) -> Result<(), Self::Error> {
        self.rename_file(old_name, new_name)
    }
}

#[derive(Debug)]
pub enum Error {
    NotFound,
    NotADir,
    NotAFile,
    NotEmpty,
    DBError(jammdb::Error),
    AlreadyExists,
    Other,
}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> core::fmt::Result {
        f.write_fmt(format_args!("{:?}", self))
    }
}

impl core::error::Error for Error{}

impl From<jammdb::Error> for Error {
    fn from(value: jammdb::Error) -> Self {
        Error::DBError(value)
    }
}
