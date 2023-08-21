use std::fs::File;

use arrow2::array::Array;
use arrow2::chunk::Chunk;
use arrow2::datatypes::Schema;
use arrow2::error::Result;
use arrow2::io::ipc::read;

#[allow(clippy::type_complexity)]
fn read_chunks(path: &str) -> Result<(Schema, Vec<Chunk<Box<dyn Array>>>)> {
    let mut file = File::open(path)?;

    let metadata = read::read_file_metadata(&mut file)?;

    let schema = metadata.schema.clone();
    let reader = read::FileReader::new(file, metadata, None, None);

    let chunks = reader.collect::<Result<Vec<_>>>()?;
    Ok((schema, chunks))
}

fn main() -> Result<()> {
    use std::env;
    let args: Vec<String> = env::args().collect();

    let file_path = &args[1];

    let (schema, chunks) = read_chunks(file_path)?;
    println!("{:?}", schema);
    println!("{:?}", chunks);

    Ok(())
}
