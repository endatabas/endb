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

#[cfg(test)]
mod tests {
    use crate::arrow;
    use arrow2::array::*;
    use arrow2::datatypes::{DataType, Field};
    use arrow2::{error::Result, ffi};

    #[test]
    fn roundtrip_ipc_buffer_unnests_struct_array() -> Result<()> {
        let array = Int32Array::from(&[Some(2), None, Some(1), None]);
        let field = Field::new("a", array.data_type().clone(), true);
        let array: Box<dyn Array> = Box::new(array);
        let struct_field = Field::new("", DataType::Struct(vec![field.clone()]), false);
        let struct_array: Box<dyn Array> = Box::new(StructArray::new(
            struct_field.data_type().clone(),
            vec![array],
            None,
        ));
        let struct_arrays = vec![
            struct_array.clone(),
            struct_array.clone(),
            struct_array.clone(),
        ];

        let iter = Box::new(struct_arrays.clone().into_iter().map(Ok));
        let mut stream = Box::new(ffi::ArrowArrayStream::empty());

        *stream = ffi::export_iterator(iter, struct_field.clone());

        let buffer = arrow::write_arrow_array_stream_to_ipc_buffer(*stream)?;
        let stream = arrow::read_arrow_array_stream_from_ipc_buffer(&buffer)?;

        let mut stream = unsafe { ffi::ArrowArrayStreamReader::try_new(Box::new(stream))? };

        let mut produced_arrays: Vec<Box<dyn Array>> = vec![];
        while let Some(array) = unsafe { stream.next() } {
            produced_arrays.push(array?);
        }

        assert_eq!(produced_arrays, struct_arrays);
        assert_eq!(stream.field(), &struct_field);

        Ok(())
    }
}
