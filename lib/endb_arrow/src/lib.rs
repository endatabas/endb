pub fn read_arrow_array_stream_from_ipc_buffer(
    buffer: &[u8],
) -> arrow::error::Result<arrow::ffi_stream::FFI_ArrowArrayStream> {
    let cursor = std::io::Cursor::new(buffer);

    let (schema, iter) = match arrow::ipc::reader::FileReader::try_new(cursor, None) {
        Err(arrow::error::ArrowError::ParseError(_)) => {
            let cursor = std::io::Cursor::new(buffer);
            let reader = arrow::ipc::reader::StreamReader::try_new(cursor, None)?;
            let schema = reader.schema();
            (schema, reader.collect::<Vec<_>>().into_iter())
        }
        Err(err) => return Err(err),
        Ok(reader) => {
            let schema = reader.schema();
            (schema, reader.collect::<Vec<_>>().into_iter())
        }
    };
    let mut stream = arrow::ffi_stream::FFI_ArrowArrayStream::empty();
    unsafe {
        arrow::ffi_stream::export_reader_into_raw(
            Box::new(arrow::record_batch::RecordBatchIterator::new(iter, schema)),
            &mut stream as *mut arrow::ffi_stream::FFI_ArrowArrayStream,
        )
    };
    Ok(stream)
}

pub fn write_arrow_array_stream_to_ipc_buffer(
    mut stream: arrow::ffi_stream::FFI_ArrowArrayStream,
    ipc_stream: bool,
) -> arrow::error::Result<Vec<u8>> {
    use arrow::record_batch::RecordBatchReader;

    let reader = unsafe { arrow::ffi_stream::ArrowArrayStreamReader::from_raw(&mut stream)? };
    let schema = reader.schema();

    if ipc_stream {
        let mut writer = arrow::ipc::writer::StreamWriter::try_new(vec![], schema.as_ref())?;

        for batch in reader {
            writer.write(&batch?)?;
        }

        writer.into_inner()
    } else {
        let mut writer = arrow::ipc::writer::FileWriter::try_new(vec![], schema.as_ref())?;

        for batch in reader {
            writer.write(&batch?)?;
        }

        writer.into_inner()
    }
}
