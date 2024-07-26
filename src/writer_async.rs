use crate::common::stream::WrappedWritableStream;
use crate::error::{ParquetWasmError, Result};
use async_compat::{Compat, CompatExt};
use futures::channel::{oneshot, oneshot::Sender};
use futures::{AsyncWriteExt, Stream, StreamExt};
use parquet::arrow::async_writer::AsyncArrowWriter;
use wasm_bindgen::{JsCast, JsValue};
use wasm_bindgen_futures::spawn_local;
use web_sys::TransformStreamDefaultController;
use crate::log;

async fn construct_writer(
    first_batch: Option<&Result<arrow_wasm::record_batch::RecordBatch>>,
    writable_stream: WrappedWritableStream<'static>,
    options: Option<parquet::file::properties::WriterProperties>,
) -> Result<AsyncArrowWriter<Compat<WrappedWritableStream<'static>>>> {
    let Some(first_batch) = first_batch else {
        // specifically handle the empty first batch case
        return Err(ParquetWasmError::PlatformSupportError("Empty first batch".to_string()));
    };
    let Ok(first_batch) = first_batch else {
        // specifically handle the decoding error case
        return Err(ParquetWasmError::PlatformSupportError("quoi".to_string()));
    };
    let schema = first_batch.schema().into_inner();
    let intermediate = writable_stream.compat();
    let writer = AsyncArrowWriter::try_new(intermediate, schema, options)?;
    Ok(writer)
}

async fn transform_stream_with_controller() -> Result<(wasm_streams::transform::sys::TransformStream, TransformStreamDefaultController)> {
    let transform_object = js_sys::Object::new();
    let prom = js_sys::Promise::new(&mut |resolve, _reject| {
        let cb = wasm_bindgen::closure::Closure::once_into_js(move |controller: JsValue| {
            let _ = resolve.call1( &JsValue::null(), &controller.into());
        });
        let _ = js_sys::Reflect::set(&transform_object, &"start".into(), cb.as_ref());
    });
    let raw_stream = wasm_streams::transform::sys::TransformStream::new_with_transformer(&transform_object).unwrap();
    let controller = wasm_bindgen_futures::JsFuture::from(prom).await.map_err(|_err| {
        ParquetWasmError::PlatformSupportError("Failed to create TransformStream".to_string())
    })?.unchecked_into();
    Ok((raw_stream, controller))
}

pub async fn generate_output_stream(
    batches: impl futures::Stream<Item = Result<arrow_wasm::RecordBatch>> + 'static,
    writer_properties: crate::writer_properties::WriterProperties,
) -> Result<wasm_streams::readable::sys::ReadableStream> {
    let options = Some(writer_properties.into());
    let intermediate_initialization= transform_stream_with_controller().await;
    if let Ok((raw_stream, controller)) = intermediate_initialization {
        let (writable_stream, output_stream) = {
            let raw_writable = raw_stream.writable();
            let inner_writer =
                wasm_streams::WritableStream::from_raw(raw_writable).into_async_write();
            let writable_stream = WrappedWritableStream {
                stream: inner_writer,
            };
            (writable_stream, raw_stream.readable())
        };
        // Errors that occur during writing have to error the stream.
        let adapted_stream = batches.peekable();
        spawn_local(async move {
            let mut pinned_stream = std::pin::pin!(adapted_stream);
            let first_batch = pinned_stream.as_mut().peek().await;
            match construct_writer(first_batch, writable_stream, options).await {
                Ok(mut writer) => {
                    while let Some(batch) = pinned_stream.next().await {
                        match batch {
                            Ok(batch) => {
                                if let Err(err) = writer.write(&batch.into()).await {
                                    controller.error_with_reason(&err.to_string().into());
                                    break;
                                };
                            },
                            Err(err) => {
                                controller.error_with_reason(&err.to_string().into());
                                break;
                            }
                        };
                    }
                    let _ = writer.close().await;
                },
                Err(err) => {
                    controller.error_with_reason(&err.to_string().into())
                }
            };
        });
        Ok(output_stream.clone())
    } else {
        Err(ParquetWasmError::PlatformSupportError(
            "Failed to create TransformStream".to_string(),
        ))
    }
}
