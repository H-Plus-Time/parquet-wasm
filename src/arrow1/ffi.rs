use std::sync::Arc;

use arrow::array::{make_array, Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::ffi::{self, from_ffi, to_ffi};
use arrow::record_batch::RecordBatch;
use wasm_bindgen::prelude::*;

use crate::arrow1::error::Result;

/// Wrapper around an ArrowArray FFI struct in Wasm memory.
#[wasm_bindgen]
pub struct FFIArrowArray(Box<ffi::FFI_ArrowArray>);

#[wasm_bindgen]
impl FFIArrowArray {
    #[wasm_bindgen]
    pub fn addr(&self) -> *const ffi::FFI_ArrowArray {
        self.0.as_ref() as *const _
    }

    #[wasm_bindgen]
    pub fn free(self) {
        drop(self.0)
    }

    #[wasm_bindgen]
    pub fn drop(self) {
        drop(self.0)
    }
}

/// Wrapper around an ArrowSchema FFI struct in Wasm memory.
#[wasm_bindgen]
pub struct FFIArrowField(Box<ffi::FFI_ArrowSchema>);

#[wasm_bindgen]
impl FFIArrowField {
    #[wasm_bindgen]
    pub fn addr(&self) -> *const ffi::FFI_ArrowSchema {
        self.0.as_ref() as *const _
    }
}

impl From<&Field> for FFIArrowField {
    fn from(value: &Field) -> Self {
        todo!()
    }
}

/// Wrapper around a collection of FFI ArrowSchema structs in Wasm memory
#[wasm_bindgen]
pub struct FFIArrowSchema(Vec<FFIArrowField>);

#[wasm_bindgen]
impl FFIArrowSchema {
    /// The number of fields in this schema
    #[wasm_bindgen]
    pub fn length(&self) -> usize {
        self.0.len()
    }

    #[wasm_bindgen]
    pub fn addr(&self, i: usize) -> *const ffi::FFI_ArrowSchema {
        self.0.get(i).unwrap().addr()
    }
}

impl From<&Schema> for FFIArrowSchema {
    fn from(value: &Schema) -> Self {
        for field in value.fields.into_iter() {}
        todo!()
    }
}

/// Wrapper to represent an Arrow Chunk in Wasm memory, e.g. a collection of FFI ArrowArray
/// structs
#[wasm_bindgen]
pub struct FFIArrowRecordBatch {
    arrays: Vec<FFIArrowArray>,
    schema: FFIArrowSchema,
}

#[wasm_bindgen]
impl FFIArrowRecordBatch {
    /// The number of columns in this record batch.
    #[wasm_bindgen(js_name = numColumns)]
    pub fn num_columns(&self) -> usize {
        self.arrays.len()
    }

    /// Get the number of Fields in the table schema
    #[wasm_bindgen(js_name = schemaLength)]
    pub fn schema_length(&self) -> usize {
        self.schema.length()
    }

    /// Get the pointer to one ArrowSchema FFI struct
    /// @param i number the index of the field in the schema to use
    #[wasm_bindgen(js_name = schemaAddr)]
    pub fn schema_addr(&self, i: usize) -> *const ffi::FFI_ArrowSchema {
        self.schema.addr(i)
    }

    /// Get the pointer to one ArrowArray FFI struct for a given chunk index and column index
    /// @param column number The column index to use
    /// @returns number pointer to an ArrowArray FFI struct in Wasm memory
    #[wasm_bindgen(js_name = arrayAddr)]
    pub fn array_addr(&self, column: usize) -> *const ffi::FFI_ArrowArray {
        self.arrays[column].addr()
    }
}

impl From<RecordBatch> for FFIArrowRecordBatch {
    fn from(value: RecordBatch) -> Self {
        let mut arrays = Vec::with_capacity(value.num_columns());
        let mut fields = Vec::with_capacity(value.num_columns());

        for column in value.columns() {
            let data = column.to_data();
            let (out_array, out_schema) = to_ffi(&data).unwrap();
            arrays.push(FFIArrowArray(Box::new(out_array)));
            fields.push(FFIArrowField(Box::new(out_schema)));
        }

        Self {
            arrays,
            schema: FFIArrowSchema(fields),
        }
    }
}

impl From<FFIArrowRecordBatch> for RecordBatch {
    fn from(value: FFIArrowRecordBatch) -> Self {
        let mut columns = Vec::with_capacity(value.schema_length());
        let mut fields = Vec::with_capacity(value.schema_length());
        for (array, schema) in value.arrays.into_iter().zip(value.schema.0.into_iter()) {
            let array = make_array(from_ffi(*array.0, &schema.0).unwrap());
            let name = schema.0.name();
            let data_type = DataType::try_from(schema.0.as_ref()).unwrap();
            let nullable = schema.0.nullable();
            columns.push(array);
            fields.push(Field::new(name, data_type, nullable))
        }

        let x = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns);
        // RecordBatch::n

        todo!()
    }
}

/// Wrapper around an Arrow Table in Wasm memory (a list of FFI ArrowSchema structs plus a list of
/// lists of ArrowArray FFI structs.)
#[wasm_bindgen]
pub struct FFIArrowTable(Vec<FFIArrowRecordBatch>);

impl From<Vec<RecordBatch>> for FFIArrowTable {
    fn from(value: Vec<RecordBatch>) -> Self {
        let mut batches = Vec::with_capacity(value.len());
        for batch in value {
            batches.push(batch.into());
        }
        Self(batches)
    }
}

#[wasm_bindgen]
impl FFIArrowTable {
    // TODO: access to
}

impl From<Vec<FFIArrowRecordBatch>> for FFIArrowTable {
    fn from(batches: Vec<FFIArrowRecordBatch>) -> Self {
        Self(batches)
    }
}

impl FFIArrowTable {
    pub fn from_iterator(value: impl IntoIterator<Item = RecordBatch>) -> Self {
        let mut batches = vec![];
        for batch in value.into_iter() {
            batches.push(batch.into());
        }
        Self(batches)
    }

    pub fn try_from_iterator(
        value: impl IntoIterator<Item = arrow::error::Result<RecordBatch>>,
    ) -> Result<Self> {
        let mut batches = vec![];
        for batch in value.into_iter() {
            batches.push(batch?.into());
        }
        Ok(Self(batches))
    }
}
