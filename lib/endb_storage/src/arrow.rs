use arrow2::array::StructArray;
use arrow2::chunk::Chunk;
use arrow2::datatypes::{DataType, Field, Schema};
use arrow2::ffi;
pub use arrow2::ffi::{ArrowArray, ArrowArrayStream, ArrowArrayStreamReader, ArrowSchema};
use arrow2::io::ipc::read;
use arrow2::io::ipc::write;

pub fn read_arrow_array_stream_from_ipc_buffer(
    buffer: &[u8],
) -> arrow2::error::Result<ArrowArrayStream> {
    let mut cursor = std::io::Cursor::new(buffer);
    let metadata = read::read_file_metadata(&mut cursor)?;
    let reader = read::FileReader::new(cursor, metadata.clone(), None, None);
    let field = Field::new("", DataType::Struct(metadata.schema.fields), false);
    let iter = Box::new(
        reader
            .map(|x| {
                x.map(|x| {
                    StructArray::new(field.data_type().clone(), x.into_arrays(), None).boxed()
                })
            })
            .collect::<Vec<_>>()
            .into_iter(),
    );
    Ok(ffi::export_iterator(iter, field))
}

pub fn write_arrow_array_stream_to_ipc_buffer(
    stream: ArrowArrayStream,
) -> arrow2::error::Result<Vec<u8>> {
    let mut reader = unsafe { ArrowArrayStreamReader::try_new(Box::new(stream))? };
    let options = write::WriteOptions { compression: None };
    let schema = match reader.field().data_type() {
        DataType::Struct(fields) => Schema::from(fields.clone()),
        _ => Schema::from(vec![reader.field().clone()]),
    };
    let mut writer = write::FileWriter::try_new(vec![], schema, None, options)?;

    while let Some(batch) = unsafe { reader.next() } {
        let batch = batch?;
        let arrays = match batch.as_any().downcast_ref::<StructArray>() {
            Some(struct_array) => struct_array.values().to_vec(),
            None => vec![batch],
        };
        let chunk = Chunk::try_new(arrays)?;
        writer.write(&chunk, None)?;
    }
    writer.finish()?;
    Ok(writer.into_inner())
}
