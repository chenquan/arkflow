//! Protobuf Processor Components
//!
//! The processor used to convert between Protobuf data and the Arrow format

use async_trait::async_trait;
use datafusion::arrow;
use datafusion::arrow::array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Float32Array, Float64Array, Int32Array, Int64Array,
    StringArray, UInt32Array, UInt64Array,
};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::parquet::data_type::AsBytes;
use prost_reflect::prost::Message;
use prost_reflect::prost_types::FileDescriptorSet;
use prost_reflect::{DynamicMessage, MessageDescriptor, Value};
use serde::{Deserialize, Serialize};
use std::path::Path;
use std::sync::Arc;
use std::{fs, io};

use arkflow_core::processor::{register_processor_builder, Processor, ProcessorBuilder};
use arkflow_core::{Error, MessageBatch};
use protobuf::Message as ProtobufMessage;

/// Protobuf format conversion processor configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
struct ProtobufProcessorConfig {
    /// Protobuf message type descriptor file path
    proto_inputs: Vec<String>,
    proto_includes: Option<Vec<String>>,
    /// Protobuf message type name
    message_type: String,
    mode: ToType,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum ToType {
    ArrowToProtobuf,
    ProtobufToArrow(ArrowConfig),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ArrowConfig {
    pub value_field: Option<String>,
}
/// Protobuf Format Conversion Processor
struct ProtobufProcessor {
    _config: ProtobufProcessorConfig,
    descriptor: MessageDescriptor,
}

impl ProtobufProcessor {
    /// Create a new Protobuf format conversion processor
    pub fn new(config: ProtobufProcessorConfig) -> Result<Self, Error> {
        // Check the file extension to see if it's a proto file or a binary descriptor file
        let file_descriptor_set = Self::parse_proto_file(&config)?;

        let descriptor_pool = prost_reflect::DescriptorPool::from_file_descriptor_set(
            file_descriptor_set,
        )
        .map_err(|e| Error::Config(format!("Unable to create Protobuf descriptor pool: {}", e)))?;

        let message_descriptor = descriptor_pool
            .get_message_by_name(&config.message_type)
            .ok_or_else(|| {
                Error::Config(format!(
                    "The message type could not be found: {}",
                    config.message_type
                ))
            })?;

        Ok(Self {
            _config: config.clone(),
            descriptor: message_descriptor,
        })
    }

    /// Parse and generate a FileDescriptorSet from the .proto file
    fn parse_proto_file(c: &ProtobufProcessorConfig) -> Result<FileDescriptorSet, Error> {
        let mut proto_inputs: Vec<String> = vec![];
        for x in &c.proto_inputs {
            let files_in_dir_result = list_files_in_dir(x)
                .map_err(|e| Error::Config(format!("Failed to list proto files: {}", e)))?;
            proto_inputs.extend(
                files_in_dir_result
                    .iter()
                    .filter(|path| path.ends_with(".proto"))
                    .map(|path| format!("{}/{}", x, path))
                    .collect::<Vec<_>>(),
            )
        }
        let proto_includes = c.proto_includes.clone().unwrap_or(c.proto_inputs.clone());

        // Parse the proto file using the protobuf_parse library
        let file_descriptor_protos = protobuf_parse::Parser::new()
            .pure()
            .inputs(proto_inputs)
            .includes(proto_includes)
            .parse_and_typecheck()
            .map_err(|e| Error::Config(format!("Failed to parse the proto file: {}", e)))?
            .file_descriptors;

        if file_descriptor_protos.is_empty() {
            return Err(Error::Config(
                "Parsing the proto file does not yield any descriptors".to_string(),
            ));
        }

        // Convert FileDescriptorProto to FileDescriptorSet
        let mut file_descriptor_set = FileDescriptorSet { file: Vec::new() };

        for proto in file_descriptor_protos {
            // Convert the protobuf library's FileDescriptorProto to a prost_types FileDescriptorProto
            let proto_bytes = proto.write_to_bytes().map_err(|e| {
                Error::Config(format!("Failed to serialize FileDescriptorProto: {}", e))
            })?;

            let prost_proto =
                prost_reflect::prost_types::FileDescriptorProto::decode(proto_bytes.as_slice())
                    .map_err(|e| {
                        Error::Config(format!("Failed to convert FileDescriptorProto: {}", e))
                    })?;

            file_descriptor_set.file.push(prost_proto);
        }

        Ok(file_descriptor_set)
    }

    /// Convert Protobuf data to Arrow format
    fn protobuf_to_arrow(&self, data: &[u8]) -> Result<RecordBatch, Error> {
        // 解析Protobuf消息
        let proto_msg = DynamicMessage::decode(self.descriptor.clone(), data)
            .map_err(|e| Error::Process(format!("Protobuf message parsing failed: {}", e)))?;

        // Building an Arrow Schema
        let mut fields = Vec::new();
        let mut columns: Vec<ArrayRef> = Vec::new();

        // Iterate over all fields of a Protobuf message
        for field in self.descriptor.fields() {
            let field_name = field.name();

            let field_value_opt = proto_msg.get_field_by_name(field_name);
            if field_value_opt.is_none() {
                continue;
            }
            let field_value = field_value_opt.unwrap();
            match field_value.as_ref() {
                Value::Bool(value) => {
                    fields.push(Field::new(field_name, DataType::Boolean, false));
                    columns.push(Arc::new(BooleanArray::from(vec![value.clone()])));
                }
                Value::I32(value) => {
                    fields.push(Field::new(field_name, DataType::Int32, false));
                    columns.push(Arc::new(Int32Array::from(vec![value.clone()])));
                }
                Value::I64(value) => {
                    fields.push(Field::new(field_name, DataType::Int64, false));
                    columns.push(Arc::new(Int64Array::from(vec![value.clone()])));
                }
                Value::U32(value) => {
                    fields.push(Field::new(field_name, DataType::UInt32, false));
                    columns.push(Arc::new(UInt32Array::from(vec![value.clone()])));
                }
                Value::U64(value) => {
                    fields.push(Field::new(field_name, DataType::UInt64, false));
                    columns.push(Arc::new(UInt64Array::from(vec![value.clone()])));
                }
                Value::F32(value) => {
                    fields.push(Field::new(field_name, DataType::Float32, false));
                    columns.push(Arc::new(Float32Array::from(vec![value.clone()])))
                }
                Value::F64(value) => {
                    fields.push(Field::new(field_name, DataType::Float64, false));
                    columns.push(Arc::new(Float64Array::from(vec![value.clone()])));
                }
                Value::String(value) => {
                    fields.push(Field::new(field_name, DataType::Utf8, false));
                    columns.push(Arc::new(StringArray::from(vec![value.clone()])));
                }
                Value::Bytes(value) => {
                    fields.push(Field::new(field_name, DataType::Binary, false));
                    columns.push(Arc::new(BinaryArray::from(vec![value.as_bytes()])));
                }
                Value::EnumNumber(value) => {
                    fields.push(Field::new(field_name, DataType::Int32, false));
                    columns.push(Arc::new(Int32Array::from(vec![value.clone()])));
                }
                _ => {
                    return Err(Error::Process(format!(
                        "Unsupported field type: {}",
                        field_name
                    )));
                } // Value::Message(_) => {}
                  // Value::List(_) => {}
                  // Value::Map(_) => {}
            }
        }

        // Create RecordBatch
        let schema = Arc::new(Schema::new(fields));
        RecordBatch::try_new(schema, columns)
            .map_err(|e| Error::Process(format!("Creating an Arrow record batch failed: {}", e)))
    }

    /// Convert Arrow format to Protobuf.
    fn arrow_to_protobuf(&self, batch: MessageBatch) -> Result<Vec<u8>, Error> {
        // Create a new dynamic message
        let mut proto_msg = DynamicMessage::new(self.descriptor.clone());

        // Get the Arrow schema.
        let schema = batch.schema();

        // Ensure there is only one line of data.
        if batch.num_rows() != 1 {
            return Err(Error::Process(
                "Only supports single-line Arrow data conversion to Protobuf.".to_string(),
            ));
        }

        for (i, field) in schema.fields().iter().enumerate() {
            let field_name = field.name();

            if let Some(proto_field) = self.descriptor.get_field_by_name(field_name) {
                let column = batch.column(i);

                match proto_field.kind() {
                    prost_reflect::Kind::Bool => {
                        if let Some(value) = column.as_any().downcast_ref::<BooleanArray>() {
                            if value.len() > 0 {
                                proto_msg
                                    .set_field_by_name(field_name, Value::Bool(value.value(0)));
                            }
                        }
                    }
                    prost_reflect::Kind::Int32
                    | prost_reflect::Kind::Sint32
                    | prost_reflect::Kind::Sfixed32 => {
                        if let Some(value) = column.as_any().downcast_ref::<Int32Array>() {
                            if value.len() > 0 {
                                proto_msg.set_field_by_name(field_name, Value::I32(value.value(0)));
                            }
                        }
                    }
                    prost_reflect::Kind::Int64
                    | prost_reflect::Kind::Sint64
                    | prost_reflect::Kind::Sfixed64 => {
                        if let Some(value) = column.as_any().downcast_ref::<Int64Array>() {
                            if value.len() > 0 {
                                proto_msg.set_field_by_name(field_name, Value::I64(value.value(0)));
                            }
                        }
                    }
                    prost_reflect::Kind::Uint32 | prost_reflect::Kind::Fixed32 => {
                        if let Some(value) = column.as_any().downcast_ref::<UInt32Array>() {
                            if value.len() > 0 {
                                proto_msg.set_field_by_name(field_name, Value::U32(value.value(0)));
                            }
                        }
                    }
                    prost_reflect::Kind::Uint64 | prost_reflect::Kind::Fixed64 => {
                        if let Some(value) = column.as_any().downcast_ref::<UInt64Array>() {
                            if value.len() > 0 {
                                proto_msg.set_field_by_name(field_name, Value::U64(value.value(0)));
                            }
                        }
                    }
                    prost_reflect::Kind::Float => {
                        if let Some(value) = column.as_any().downcast_ref::<Float32Array>() {
                            if value.len() > 0 {
                                proto_msg.set_field_by_name(field_name, Value::F32(value.value(0)));
                            }
                        }
                    }
                    prost_reflect::Kind::Double => {
                        if let Some(value) = column.as_any().downcast_ref::<Float64Array>() {
                            if value.len() > 0 {
                                proto_msg.set_field_by_name(field_name, Value::F64(value.value(0)));
                            }
                        }
                    }
                    prost_reflect::Kind::String => {
                        if let Some(value) = column.as_any().downcast_ref::<StringArray>() {
                            if value.len() > 0 {
                                proto_msg.set_field_by_name(
                                    field_name,
                                    Value::String(value.value(0).to_string()),
                                );
                            }
                        }
                    }
                    prost_reflect::Kind::Bytes => {
                        if let Some(value) = column.as_any().downcast_ref::<BinaryArray>() {
                            if value.len() > 0 {
                                proto_msg.set_field_by_name(
                                    field_name,
                                    Value::Bytes(value.value(0).to_vec().into()),
                                );
                            }
                        }
                    }
                    prost_reflect::Kind::Enum(_) => {
                        if let Some(value) = column.as_any().downcast_ref::<Int32Array>() {
                            if value.len() > 0 {
                                proto_msg.set_field_by_name(
                                    field_name,
                                    Value::EnumNumber(value.value(0)),
                                );
                            }
                        }
                    }
                    _ => {
                        return Err(Error::Process(format!(
                            "Unsupported Protobuf type: {:?}",
                            proto_field.kind()
                        )))
                    }
                }
            }
        }

        let mut buf = Vec::new();
        proto_msg
            .encode(&mut buf)
            .map_err(|e| Error::Process(format!("Protobuf encoding failed: {}", e)))?;

        Ok(buf)
    }
}

#[async_trait]
impl Processor for ProtobufProcessor {
    async fn process(&self, msg: MessageBatch) -> Result<Vec<MessageBatch>, Error> {
        if msg.is_empty() {
            return Ok(vec![]);
        }
        match self._config.mode {
            ToType::ArrowToProtobuf => {
                // Convert Arrow format to Protobuf.
                let proto_data = self.arrow_to_protobuf(msg)?;
                let new_msg = MessageBatch::new_binary(vec![proto_data])?;

                Ok(vec![new_msg])
            }
            ToType::ProtobufToArrow(ref c) => {
                if msg.is_empty() {
                    return Ok(vec![]);
                }

                let mut batches = Vec::with_capacity(msg.len());
                let result = msg.to_binary(c.value_field.as_deref().unwrap_or("value"))?;
                for x in result {
                    // Convert Protobuf messages to Arrow format.
                    let batch = self.protobuf_to_arrow(x)?;
                    batches.push(batch)
                }

                let schema = batches[0].schema();
                let batch = arrow::compute::concat_batches(&schema, &batches)
                    .map_err(|e| Error::Process(format!("Batch merge failed: {}", e)))?;
                Ok(vec![MessageBatch::new_arrow(batch)])
            }
        }
    }

    async fn close(&self) -> Result<(), Error> {
        Ok(())
    }
}

fn list_files_in_dir<P: AsRef<Path>>(dir: P) -> io::Result<Vec<String>> {
    let mut files = Vec::new();
    if dir.as_ref().is_dir() {
        for entry in fs::read_dir(dir)? {
            let entry = entry?;
            let path = entry.path();
            if path.is_file() {
                if let Some(file_name) = path.file_name() {
                    if let Some(file_name_str) = file_name.to_str() {
                        files.push(file_name_str.to_string());
                    }
                }
            }
        }
    }
    Ok(files)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ProtobufToArrowProcessorConfig {
    #[serde(flatten)]
    c: CommonProtobufProcessorConfig,
    value_field: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CommonProtobufProcessorConfig {
    proto_inputs: Vec<String>,
    proto_includes: Option<Vec<String>>,
    /// Protobuf message type name
    message_type: String,
}

type ArrowToProtobufProcessorConfig = CommonProtobufProcessorConfig;

impl From<ArrowToProtobufProcessorConfig> for ProtobufProcessorConfig {
    fn from(config: CommonProtobufProcessorConfig) -> Self {
        Self {
            proto_inputs: config.proto_inputs,
            proto_includes: config.proto_includes,
            message_type: config.message_type,
            mode: ToType::ArrowToProtobuf,
        }
    }
}

impl From<ProtobufToArrowProcessorConfig> for ProtobufProcessorConfig {
    fn from(config: ProtobufToArrowProcessorConfig) -> Self {
        Self {
            proto_inputs: config.c.proto_inputs,
            proto_includes: config.c.proto_includes,
            message_type: config.c.message_type,
            mode: ToType::ProtobufToArrow(ArrowConfig {
                value_field: config.value_field,
            }),
        }
    }
}

struct ProtobufToArrowProcessorBuilder;
impl ProcessorBuilder for ProtobufToArrowProcessorBuilder {
    fn build(&self, config: &Option<serde_json::Value>) -> Result<Arc<dyn Processor>, Error> {
        if config.is_none() {
            return Err(Error::Config(
                "ProtobufToArrow processor configuration is missing".to_string(),
            ));
        }
        let config: ProtobufToArrowProcessorConfig =
            serde_json::from_value(config.clone().unwrap())?;
        Ok(Arc::new(ProtobufProcessor::new(config.into())?))
    }
}
struct ArrowToProtobufProcessorBuilder;
impl ProcessorBuilder for ArrowToProtobufProcessorBuilder {
    fn build(&self, config: &Option<serde_json::Value>) -> Result<Arc<dyn Processor>, Error> {
        if config.is_none() {
            return Err(Error::Config(
                "ArrowToProtobuf processor configuration is missing".to_string(),
            ));
        }
        let config: ArrowToProtobufProcessorConfig =
            serde_json::from_value(config.clone().unwrap())?;
        Ok(Arc::new(ProtobufProcessor::new(config.into())?))
    }
}

pub fn init() {
    register_processor_builder(
        "arrow_to_protobuf",
        Arc::new(ArrowToProtobufProcessorBuilder),
    );
    register_processor_builder(
        "protobuf_to_arrow",
        Arc::new(ProtobufToArrowProcessorBuilder),
    );
}
