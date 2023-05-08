use arrow2::error::Error;
use arrow2::io::parquet::read;
use std::fs::File;

fn main() -> Result<(), Error> {
    use std::env;
    let args: Vec<String> = env::args().collect();
    let file_path = &args[1];
    let mut reader = File::open(file_path)?;

    let metadata = read::read_metadata(&mut reader)?;

    let schema = read::infer_schema(&metadata)?;

    println!("{schema:#?}");

    let row_groups = metadata
        .row_groups
        .into_iter()
        .enumerate()
        .map(|(_, row_group)| row_group)
        .collect();

    let chunks = read::FileReader::new(reader, row_groups, schema, Some(1024 * 8 * 8), None, None);

    for maybe_chunk in chunks {
        let chunk = maybe_chunk?;
        println!("{chunk:#?}");
    }
    Ok(())
}
