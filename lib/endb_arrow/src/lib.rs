pub fn read_arrow_array_stream_from_ipc_buffer(
    buffer: &[u8],
) -> arrow::error::Result<arrow::ffi_stream::FFI_ArrowArrayStream> {
    let cursor = std::io::Cursor::new(buffer);
    let reader = arrow::ipc::reader::FileReader::try_new(cursor, None)?;
    let schema = reader.schema();
    let iter = reader.collect::<Vec<_>>().into_iter();
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
) -> arrow::error::Result<Vec<u8>> {
    use arrow::record_batch::RecordBatchReader;

    let reader = unsafe { arrow::ffi_stream::ArrowArrayStreamReader::from_raw(&mut stream)? };
    let schema = reader.schema();
    let mut writer = arrow::ipc::writer::FileWriter::try_new(vec![], schema.as_ref())?;

    for batch in reader {
        writer.write(&batch?)?;
    }

    writer.finish()?;
    writer.into_inner()
}
