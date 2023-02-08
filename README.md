# dbfs

database as filesystem

## example

```rust
let db = DB::<Mmap>::open::<FileOpenOptions,_>("my-database.db").unwrap();
let fs = DbFileSystem::new(db);
let root = fs.root();
println!("{:?}",root);
for i in 0..10{
    root.create_file(format!("file{}",i).as_str()).unwrap();
}

root.list().iter().for_each(|x| println!("{:?}",x));
let file = root.open("file1").unwrap();
println!("{:?}",file);

file.write(0,b"hello world").unwrap();
let data = file.read(0,20).unwrap();
println!("data: {}",String::from_utf8(data).unwrap());

file.write(20,b"hello world").unwrap();
let data = file.read(0,31).unwrap();
println!("data size: {}",data.len());
println!("data: {}",String::from_utf8(data).unwrap());

for i in 0..10{
    root.rename_file(format!("file{}",i).as_str(),format!("new_file{}",i).as_str()).unwrap();
}
let new_file = root.open("new_file1").unwrap();
let size= new_file.size();
println!("file size: {}",size);

root.list().iter().for_each(|x| println!("{:?}",x));

for i in 0..9{
    root.delete_file(format!("new_file{}",i).as_str()).unwrap();
}
root.list().iter().for_each(|x| println!("{:?}",x));
```



## target

在rCore-N中完成实验

- [ ] 基本功能实现
- [ ] 学习 路径，文件，目录统一思想(主要)
- [ ] 异步（）
