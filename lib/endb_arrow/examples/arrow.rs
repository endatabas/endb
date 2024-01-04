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

    let column_idx = 0;
    let row_idx = 0;
    let sf = arrow::row::SortField::new(schema.fields.get(column_idx).unwrap().data_type().clone());
    let rc = arrow::row::RowConverter::new(vec![sf])?;

    let rows = rc.convert_columns(&[chunks
        .first()
        .unwrap()
        .as_ref()
        .unwrap()
        .column(column_idx)
        .clone()])?;
    println!("{:?}", rows.row(row_idx));

    Ok(())
}
