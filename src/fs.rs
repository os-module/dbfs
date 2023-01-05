//! DBFS
//!
//! 使用jammdb数据库构建文件系统,jammdb的底层文件依赖需要
//! 将一个存储设备模拟为一个文件,并且需要支持mmap
use alloc::string::{String, ToString};
use alloc::sync::Arc;
use alloc::vec::Vec;
use alloc::{format, vec};
use core::cmp::min;
use core::fmt::{Debug, Display, Formatter};
use fat32_trait::{DirectoryLike, FileLike};
use jammdb::{Data, DB};
use log::info;

pub struct SafeDb(DB);
unsafe impl Sync for SafeDb {}
unsafe impl Send for SafeDb {}

pub struct File {
    db: Arc<SafeDb>,
    path: String,
}

impl File {
    pub fn new(db: Arc<SafeDb>, path: &str) -> Self {
        Self {
            db,
            path: path.to_string(),
        }
    }
}
impl Debug for File {
    fn fmt(&self, f: &mut Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("File").field("path", &self.path).finish()
    }
}

pub struct Dir {
    db: Arc<SafeDb>,
    path: String,
}

impl Dir {
    pub fn new(db: Arc<SafeDb>, path: String) -> Self {
        Self { db, path }
    }
}

impl Debug for Dir {
    fn fmt(&self, f: &mut Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("DirEntry")
            .field("path", &self.path)
            .finish()
    }
}

impl FileLike for File {
    type Error = Error;

    fn read(&self, offset: u32, size: u32) -> Result<Vec<u8>, Self::Error> {
        let tx = self.db.0.tx(false)?;
        let bucket = tx.get_bucket(self.path.as_str())?;
        // where is the offset?
        let data = bucket.get("data").unwrap();
        let data = data.kv().value().to_vec();
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
        let bucket = tx.get_bucket(self.path.as_str())?;
        let data = bucket.get("data").unwrap();
        let mut data = data.kv().value().to_vec();
        if (data.len() as u32) < offset {
            data.resize(offset as usize, 0);
            for _ in data.len()..offset as usize {
                data.push(0);
            }
        }
        data.extend_from_slice(&w_data);
        bucket.put("data", data)?;
        tx.commit()?;
        Ok(w_data.len() as u32)
    }

    fn clear(&self) {
        let tx = self.db.0.tx(true).unwrap();
        let bucket = tx.get_bucket(self.path.as_str()).unwrap();
        bucket.put("data", vec![]).unwrap();
        tx.commit().unwrap();
    }

    fn size(&self) -> u32 {
        let tx = self.db.0.tx(false).unwrap();
        let bucket = tx.get_bucket(self.path.as_str()).unwrap();
        let data = bucket.get("data").unwrap();
        let data = data.kv().value().len();
        data as u32
    }
}

impl DirectoryLike for Dir {
    type Error = Error;
    /// 所有文件和目录位于虚拟的根目录下
    fn create_dir(&self, name: &str) -> Result<(), Self::Error> {
        let tx = self.db.0.tx(true)?;
        let bucket = tx.get_bucket(self.path.as_str())?;
        let bucket = bucket.get_bucket("data")?;
        let insert = bucket.get_kv(name.to_string() + "-d");
        // 检查当前目录下是否存在同名目录
        let ans = if insert.is_some() {
            Err(Error::AlreadyExists)
        } else {
            // 创建目录
            bucket.put(name.to_string() + "-d", "")?;
            // 需要保证在根目录下的目录名唯一
            let l = self.path.len();
            let unique_name = self.path[0..l - 2].to_string() + "/" + name + "-d";
            // 根目录下创建目录
            let bucket = tx.create_bucket(unique_name.as_str())?;
            // 创建目录下的data文件
            // data文件存放目录下的子文件
            bucket.create_bucket("data")?;
            Ok(())
        };
        tx.commit()?;
        ans
    }

    fn create_file(&self, name: &str) -> Result<(), Self::Error> {
        info!("create file:{} in {}", name, self.path);
        let tx = self.db.0.tx(true)?;
        let bucket = tx.get_bucket(self.path.as_str())?;
        let bucket = bucket.get_bucket("data")?;
        let insert = bucket.get_kv(name.to_string() + "-f");
        // 检查当前目录下是否存在同名文件
        let ans = if insert.is_some() {
            Err(Error::AlreadyExists)
        } else {
            // 创建文件
            bucket.put(name.to_string() + "-f", "")?;
            // 需要保证在根目录下的文件名唯一
            let l = self.path.len();
            let unique_name = self.path[0..l - 2].to_string() + "/" + name + "-f";
            // 根目录下创建文件
            let bucket = tx.create_bucket(unique_name.as_str())?;
            // 创建文件下的data文件
            // data文件存放文件内容
            bucket.put("data", "")?;
            Ok(())
        };
        tx.commit()?;
        ans
    }

    fn delete_dir(&self, name: &str) -> Result<(), Self::Error> {
        let tx = self.db.0.tx(true)?;
        let bucket = tx.get_bucket(self.path.as_str())?;
        let bucket = bucket.get_bucket("data")?;
        let delete = bucket.get_kv(name.to_string() + "-d");
        // 检查当前目录下是否存在同名目录
        let ans = if delete.is_some() {
            // 删除目录
            bucket.delete(name.to_string() + "-d")?;
            // 需要保证在根目录下的目录名唯一
            let l = self.path.len();
            let unique_name = self.path[0..l - 2].to_string() + "/" + name + "-d";
            // 根目录下删除目录
            tx.delete_bucket(unique_name.as_str())?;
            Ok(())
        } else {
            Err(Error::NotFound)
        };
        tx.commit()?;
        ans
    }

    fn delete_file(&self, name: &str) -> Result<(), Self::Error> {
        let tx = self.db.0.tx(true)?;
        let bucket = tx.get_bucket(self.path.as_str())?;
        let bucket = bucket.get_bucket("data")?;
        let delete = bucket.get_kv(name.to_string() + "-f");
        // 检查当前目录下是否存在同名目录
        let ans = if delete.is_some() {
            // 删除目录
            bucket.delete(name.to_string() + "-f").unwrap();
            // 需要保证在根目录下的目录名唯一
            let l = self.path.len();
            let unique_name = self.path[0..l - 2].to_string() + "/" + name + "-f";
            // 根目录下删除目录
            tx.delete_bucket(unique_name.as_str())?;
            Ok(())
        } else {
            Err(Error::NotFound)
        };
        tx.commit()?;
        ans
    }

    fn cd(&self, name: &str) -> Result<Arc<dyn DirectoryLike<Error = Self::Error>>, Self::Error> {
        let tx = self.db.0.tx(false)?;
        let bucket = tx.get_bucket(self.path.as_str())?;
        let bucket = bucket.get_bucket("data")?;
        let insert = bucket.get_kv(name.to_string() + "-d");
        // 检查当前目录下是否存在同名目录
        return if insert.is_some() {
            let l = self.path.len();
            let new_path = self.path[0..l - 2].to_string() + "/" + name + "-d";
            Ok(Arc::new(Dir::new(self.db.clone(), new_path)))
        } else {
            Err(Error::NotFound)
        };
    }

    fn open(&self, name: &str) -> Result<Arc<dyn FileLike<Error = Self::Error>>, Self::Error> {
        let tx = self.db.0.tx(false)?;
        let bucket = tx.get_bucket(self.path.as_str())?;
        let bucket = bucket.get_bucket("data")?;
        let insert = bucket.get_kv(name.to_string() + "-f");
        // 检查当前目录下是否存在同名目录
        return if insert.is_some() {
            let l = self.path.len();
            let new_path = self.path[0..l - 2].to_string() + "/" + name + "-f";
            let new_entry = File {
                path: new_path,
                db: self.db.clone(),
            };
            Ok(Arc::new(new_entry))
        } else {
            Err(Error::NotFound)
        };
    }

    fn list(&self) -> Result<Vec<String>, Self::Error> {
        let tx = self.db.0.tx(false)?;
        let bucket = tx.get_bucket(self.path.as_str())?;
        let bucket = bucket.get_bucket("data")?;
        let mut list = Vec::new();
        bucket.cursor().into_iter().for_each(|data| {
            let name = match &*data {
                Data::KeyValue(kv) => kv.key(),
                _ => panic!("no bucket"),
            };
            let name = core::str::from_utf8(name).unwrap();
            list.push(name[0..name.len() - 2].to_string());
        });
        Ok(list)
    }

    fn rename_file(&self, old_name: &str, new_name: &str) -> Result<(), Self::Error> {
        if old_name == new_name {
            return Ok(());
        }
        let tx = self.db.0.tx(true)?;
        let r_bucket = tx.get_bucket(self.path.as_str())?;
        let r_bucket = r_bucket.get_bucket("data")?;
        let old = r_bucket.get_kv(old_name.to_string() + "-f");
        let new = r_bucket.get_kv(new_name.to_string() + "-f");
        let ans = if old.is_some() {
            if new.is_some() {
                Err(Error::AlreadyExists)
            } else {
                let l = self.path.len();
                let old_path = self.path[0..l - 2].to_string() + "/" + old_name + "-f";
                let new_path = self.path[0..l - 2].to_string() + "/" + new_name + "-f";
                r_bucket.delete(old_name.to_string() + "-f")?;
                r_bucket.put(new_name.to_string() + "-f", "")?;
                let old_bucket = tx.get_bucket(&old_path)?;
                let old_data = old_bucket.get_kv("data").unwrap();
                let old_data = old_data.value();
                tx.delete_bucket(old_path)?;
                let new_bucket = tx.create_bucket(new_path)?;
                new_bucket.put("data", old_data)?;
                Ok(())
            }
        } else {
            Err(Error::NotFound)
        };
        tx.commit().unwrap();
        ans
    }

    fn rename_dir(&self, old_name: &str, new_name: &str) -> Result<(), Self::Error> {
        if old_name == new_name {
            return Ok(());
        }
        let tx = self.db.0.tx(true)?;
        let r_bucket = tx.get_bucket(self.path.as_str())?;
        let r_bucket = r_bucket.get_bucket("data")?;
        let old = r_bucket.get_kv(old_name.to_string() + "-d");
        let new = r_bucket.get_kv(new_name.to_string() + "-d");
        let ans = if old.is_some() {
            if new.is_some() {
                Err(Error::AlreadyExists)
            } else {
                let l = self.path.len();
                let old_path = self.path[0..l - 2].to_string() + "/" + old_name + "-d";
                let new_path = self.path[0..l - 2].to_string() + "/" + new_name + "-d";
                r_bucket.delete(old_name)?;
                r_bucket.put(new_name.to_string() + "-d", "")?;
                let old_bucket = tx.get_bucket(&old_path)?;
                let old_data = old_bucket.get_kv("data").unwrap();
                let old_data = old_data.value();
                tx.delete_bucket(old_path)?;
                let new_bucket = tx.create_bucket(new_path)?;
                new_bucket.put("data", old_data)?;
                Ok(())
            }
        } else {
            Err(Error::NotFound)
        };
        tx.commit().unwrap();
        ans
    }
}

#[derive(Debug)]
pub enum Error {
    NotFound,
    DBError(jammdb::Error),
    AlreadyExists,
    Other,
}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> core::fmt::Result {
        f.write_fmt(format_args!("{:?}", self))
    }
}

impl core::error::Error for Error {}

impl From<jammdb::Error> for Error {
    fn from(value: jammdb::Error) -> Self {
        Error::DBError(value)
    }
}
pub struct DbFileSystem {
    db: Arc<SafeDb>,
}

impl DbFileSystem {
    /// init a new filesystem
    pub fn new(db: DB) -> Self {
        Self {
            db: Arc::new(SafeDb(db)),
        }
    }
    pub fn root(&self) -> Arc<Dir> {
        // 检查根目录是否存在
        let tx = self.db.0.tx(true).unwrap();
        info!("check root exist");
        let root = tx.get_or_create_bucket("root-d");
        assert!(root.is_ok());
        root.unwrap().get_or_create_bucket("data").unwrap();
        tx.commit().unwrap();
        Arc::new(Dir::new(self.db.clone(), "root-d".to_string()))
    }
}

#[allow(unused)]
pub fn dbfs_test(db: DB) {
    let fs = DbFileSystem::new(db);
    let root = fs.root();
    info!("{:?}", root);
    for i in 0..10 {
        root.create_file(format!("file{}", i).as_str()).unwrap();
    }

    root.list().iter().for_each(|x| info!("{:?}", x));
    let file = root.open("file1").unwrap();
    info!("{:?}", file);

    file.write(0, b"hello world").unwrap();
    let data = file.read(0, 20).unwrap();
    info!("data: {}", String::from_utf8(data).unwrap());

    file.write(20, b"hello world").unwrap();
    let data = file.read(0, 31).unwrap();
    info!("data size: {}", data.len());
    info!("data: {}", String::from_utf8(data).unwrap());

    for i in 0..10 {
        root.rename_file(
            format!("file{}", i).as_str(),
            format!("new_file{}", i).as_str(),
        )
        .unwrap();
    }
    let new_file = root.open("new_file1").unwrap();
    let size = new_file.size();
    info!("file size: {}", size);

    root.list().iter().for_each(|x| info!("{:?}", x));

    for i in 0..9 {
        root.delete_file(format!("new_file{}", i).as_str()).unwrap();
    }
    root.list().iter().for_each(|x| info!("{:?}", x));
}
