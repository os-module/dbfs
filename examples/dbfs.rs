
use fat32_trait::DirectoryLike;
use logger::init_logger;
use dbfs::FileSystem;

fn main(){
    init_logger();
    let fs = FileSystem::init();
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


}


