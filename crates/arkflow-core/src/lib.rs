//! Rust stream processing engine

use datafusion::arrow::array::{ArrayRef, BinaryArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::parquet::data_type::AsBytes;
use serde::Serialize;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;
use thiserror::Error;

pub mod buffer;
pub mod cli;
pub mod config;
pub mod engine;
pub mod input;
pub mod output;
pub mod pipeline;
pub mod processor;

pub mod stream;

/// Error in the stream processing engine
#[derive(Error, Debug)]
pub enum Error {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    #[error("Configuration error: {0}")]
    Config(String),

    #[error("Read error: {0}")]
    Read(String),

    #[error("Process errors: {0}")]
    Process(String),

    #[error("Connection error: {0}")]
    Connection(String),

    /// Reconnection should be attempted after a connection loss.
    #[error("Connection lost")]
    Disconnection,

    #[error("Timeout error")]
    Timeout,

    #[error("Unknown error: {0}")]
    Unknown(String),

    #[error("EOF")]
    EOF,
}

pub type Bytes = Vec<u8>;

/// Represents a message in a stream processing engine.
#[derive(Clone, Debug)]
pub struct MessageBatch(RecordBatch);

impl MessageBatch {
    pub fn new_binary(content: Vec<Bytes>) -> Result<Self, Error> {
        let fields = vec![Field::new("value", DataType::Binary, false)];
        let mut columns: Vec<ArrayRef> = Vec::with_capacity(content.len());

        let bytes: Vec<_> = content.iter().map(|x| x.as_bytes()).collect();

        let array = BinaryArray::from_vec(bytes);
        columns.push(Arc::new(array));

        let schema = Arc::new(Schema::new(fields));
        let batch = RecordBatch::try_new(schema, columns)
            .map_err(|e| Error::Process(format!("Creating an Arrow record batch failed: {}", e)))?;

        Ok(Self(batch))
    }

    pub fn from_json<T: Serialize>(value: &T) -> Result<Self, Error> {
        let content = serde_json::to_vec(value)?;
        Ok(Self::new_binary(vec![content])?)
    }

    pub fn new_arrow(content: RecordBatch) -> Self {
        Self(content)
    }

    /// Create a message from a string.
    pub fn from_string(content: &str) -> Result<Self, Error> {
        Self::new_binary(vec![content.as_bytes().to_vec()])
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn len(&self) -> usize {
        self.0.num_rows()
    }

    pub fn to_binary(&self, name: &str) -> Result<Vec<&[u8]>, Error> {
        let Some(array_ref) = self.0.column_by_name(name) else {
            return Err(Error::Process("not found column".to_string()));
        };

        let data = array_ref.to_data();

        if *data.data_type() != DataType::Binary {
            return Err(Error::Process("not support data type".to_string()));
        }

        let Some(v) = array_ref.as_any().downcast_ref::<BinaryArray>() else {
            return Err(Error::Process("not support data type".to_string()));
        };
        let mut vec_bytes = vec![];
        for x in v {
            if let Some(data) = x {
                vec_bytes.push(data)
            }
        }
        Ok(vec_bytes)
    }
}

impl Deref for MessageBatch {
    type Target = RecordBatch;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl From<RecordBatch> for MessageBatch {
    fn from(batch: RecordBatch) -> Self {
        Self(batch)
    }
}

impl From<MessageBatch> for RecordBatch {
    fn from(batch: MessageBatch) -> Self {
        batch.0
    }
}

impl TryFrom<Vec<Bytes>> for MessageBatch {
    type Error = Error;

    fn try_from(value: Vec<Bytes>) -> Result<Self, Self::Error> {
        Self::new_binary(value)
    }
}

impl TryFrom<Vec<String>> for MessageBatch {
    type Error = Error;

    fn try_from(value: Vec<String>) -> Result<Self, Self::Error> {
        Self::new_binary(value.into_iter().map(|s| s.into_bytes()).collect())
    }
}

impl TryFrom<Vec<&str>> for MessageBatch {
    type Error = Error;

    fn try_from(value: Vec<&str>) -> Result<Self, Self::Error> {
        Self::new_binary(value.into_iter().map(|s| s.as_bytes().to_vec()).collect())
    }
}

impl DerefMut for MessageBatch {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}
