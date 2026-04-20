use tokio_util::codec::{Decoder, Encoder, LengthDelimitedCodec};
use std::io;
use std::sync::Arc;
use bytes::{Bytes, BytesMut};
use serde::{de::DeserializeOwned, Serialize};

#[derive(Debug)]
pub struct EnCodec<I> (pub LengthDelimitedCodec, std::marker::PhantomData<I>);

impl<I> EnCodec<I> {
    pub fn new() -> Self {
        EnCodec(LengthDelimitedCodec::new(),std::marker::PhantomData::<I>)
    }
}

impl<I> std::clone::Clone for EnCodec<I> {
    fn clone(&self) -> Self {
        EnCodec::new()
    }
}

impl<I> Encoder<I> for EnCodec<I>
where I: Serialize,
{
    type Error = io::Error;

    fn encode(&mut self, item: I, dst:&mut BytesMut) -> Result<(),Self::Error> {
        let data = bincode::serialize(&item)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        let buf = Bytes::from(data);
        return self.0.encode(buf, dst);
    }
}

// Arc bypass: the consensus reactors hand the codec an `Arc<ProtocolMsg>`
// so broadcasts can share the payload. Serialize the inner value directly
// instead of requiring serde's `rc` feature.
impl<I> Encoder<Arc<I>> for EnCodec<I>
where I: Serialize,
{
    type Error = io::Error;

    fn encode(&mut self, item: Arc<I>, dst:&mut BytesMut) -> Result<(),Self::Error> {
        let data = bincode::serialize(item.as_ref())
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        let buf = Bytes::from(data);
        return self.0.encode(buf, dst);
    }
}

pub struct Decodec<O> (pub LengthDelimitedCodec, std::marker::PhantomData<O>);
impl<O> Decodec<O> {
    pub fn new() -> Self {
        Decodec(LengthDelimitedCodec::new(),std::marker::PhantomData::<O>)
    }
}

impl<O> Decoder for Decodec<O>
where O: DeserializeOwned,
{
    type Item = O;
    type Error = io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        match self.0.decode(src)? {
            Some(in_data) => {
                let item = bincode::deserialize(&in_data)
                    .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
                Ok(Some(item))
            },
            None => Ok(None),
        }
    }
}

impl<O> std::clone::Clone for Decodec<O>
{
    fn clone(&self) -> Self {
        Decodec::new()
    }
}
