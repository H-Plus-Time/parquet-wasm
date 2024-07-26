use futures::{ready, AsyncWrite};
use wasm_bindgen::JsValue;
use crate::log;
pub struct WrappedWritableStream<'writer> {
    pub stream: wasm_streams::writable::IntoAsyncWrite<'writer>,
}

impl<'writer> WrappedWritableStream<'writer> {
    pub async fn abort(self) -> Result<(), JsValue> {
        self.stream.abort().await
    }
    pub async fn abort_with_reason(self, reason: &JsValue) -> Result<(), JsValue> {
        self.stream.abort_with_reason(reason).await
    }
}

impl<'writer> AsyncWrite for WrappedWritableStream<'writer> {
    fn poll_write(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        // log!("Underlying write");
        AsyncWrite::poll_write(std::pin::Pin::new(&mut self.get_mut().stream), cx, buf)
    }

    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        // log!("Underlying flush");
        AsyncWrite::poll_flush(std::pin::Pin::new(&mut self.get_mut().stream), cx)
    }

    fn poll_close(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        AsyncWrite::poll_close(std::pin::Pin::new(&mut self.get_mut().stream), cx)
    }
}

unsafe impl<'writer> Send for WrappedWritableStream<'writer> {}
