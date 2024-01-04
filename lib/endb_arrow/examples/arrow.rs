use std::fs::File;

fn main() -> arrow::error::Result<()> {
    use std::env;
    let args: Vec<String> = env::args().collect();

    let file_path = &args[1];

    let file = File::open(file_path)?;

    let reader = arrow::ipc::reader::FileReader::try_new(file, None)?;
    let schema = reader.schema();
    let chunks = reader.collect::<Vec<_>>();

    println!("{:?}", schema);
    println!("{:?}", chunks);

    Ok(())
}
