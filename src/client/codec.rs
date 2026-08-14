use bytes::{Buf, BufMut, BytesMut};
use rabbitmq_stream_protocol::{error::DecodeError, Request, Response};
use tokio_util::codec::{Decoder as TokioDecoder, Encoder as TokioEncoder};

use rabbitmq_stream_protocol::codec::{Decoder, Encoder};

use crate::error::ClientError;

/// Capacity above which an EMPTY codec buffer is released back to the
/// allocator instead of being kept for reuse.
///
/// `BytesMut` only ever grows: encoding one large frame (or receiving one
/// large chunk) permanently pins that capacity on the connection, and a
/// producer/consumer holds one connection per stream partition — so buffer
/// high-water marks accumulate across partitions for the life of the process.
/// Reclaiming oversized buffers once they are empty turns a large frame into
/// a transient allocation instead of a permanent one.
///
/// Configured via `RABBITMQ_STREAM_CLIENT_BUFFER_RECLAIM_BYTES`
/// (default 1 MiB). Buffers at or below the threshold are always kept, so
/// steady-state traffic never churns allocations.
pub(crate) fn buffer_reclaim_threshold() -> usize {
    static THRESHOLD: std::sync::OnceLock<usize> = std::sync::OnceLock::new();
    *THRESHOLD.get_or_init(|| {
        std::env::var("RABBITMQ_STREAM_CLIENT_BUFFER_RECLAIM_BYTES")
            .ok()
            .and_then(|value| value.trim().parse::<usize>().ok())
            .filter(|&bytes| bytes > 0)
            .unwrap_or(1024 * 1024)
    })
}

/// Release an empty, oversized buffer. No-op unless the buffer is empty (a
/// non-empty buffer holds bytes the transport still owns) and its capacity
/// exceeds the reclaim threshold.
fn maybe_reclaim(buf: &mut BytesMut) {
    if buf.is_empty() && buf.capacity() > buffer_reclaim_threshold() {
        *buf = BytesMut::new();
    }
}

#[derive(Debug)]
pub(crate) struct RabbitMqStreamCodec {}

impl TokioDecoder for RabbitMqStreamCodec {
    type Item = Response;
    type Error = ClientError;

    fn decode(&mut self, buf: &mut BytesMut) -> Result<Option<Response>, ClientError> {
        match Response::decode(buf) {
            Ok((remaining, response)) => {
                let len = remaining.len();
                buf.advance(buf.len() - len);
                // A fully-drained read buffer that grew for a large chunk
                // must not keep that capacity for the connection's lifetime.
                maybe_reclaim(buf);
                Ok(Some(response))
            }
            Err(DecodeError::Incomplete(_)) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }
}

impl TokioEncoder<Request> for RabbitMqStreamCodec {
    type Error = ClientError;

    fn encode(&mut self, req: Request, buf: &mut BytesMut) -> Result<(), ClientError> {
        // The write buffer was drained by the previous flush; if a past frame
        // grew it beyond the threshold, release it before reserving for this
        // frame so capacity tracks the CURRENT frame, not the historical max.
        maybe_reclaim(buf);
        let len = req.encoded_size();
        buf.reserve(len as usize);
        let mut writer = buf.writer();
        req.encode(&mut writer)?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reclaim_releases_empty_oversized_buffer() {
        let mut buf = BytesMut::with_capacity(buffer_reclaim_threshold() * 4);
        maybe_reclaim(&mut buf);
        assert_eq!(buf.capacity(), 0, "empty oversized buffer must be released");
    }

    #[test]
    fn reclaim_keeps_buffer_at_or_below_threshold() {
        let mut buf = BytesMut::with_capacity(buffer_reclaim_threshold());
        let capacity = buf.capacity();
        maybe_reclaim(&mut buf);
        assert_eq!(
            buf.capacity(),
            capacity,
            "threshold-sized buffer must be kept for reuse"
        );
    }

    #[test]
    fn reclaim_never_touches_a_non_empty_buffer() {
        let mut buf = BytesMut::with_capacity(buffer_reclaim_threshold() * 4);
        buf.extend_from_slice(b"pending bytes the transport still owns");
        let capacity = buf.capacity();
        maybe_reclaim(&mut buf);
        assert_eq!(buf.capacity(), capacity);
        assert_eq!(&buf[..], b"pending bytes the transport still owns");
    }
}
