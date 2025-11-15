use crate::pre_serialized_values::{
    CsharpMemoryHandler, CsharpValuePtr, HasCells, PreSerializedValuesTrait,
};
use scylla::serialize::SerializationError;
use scylla::serialize::row::{RowSerializationContext, SerializeRow};
use scylla::serialize::writers::RowWriter;

pub struct OwnedPreSerializedValues {
    cells: Vec<BufferOwningCell>,
}

#[derive(Clone)]
pub(crate) enum BufferOwningCell {
    Bytes(Vec<u8>),
    Null,
    Unset,
}

impl OwnedPreSerializedValues {
    pub fn new() -> Self {
        Self { cells: Vec::new() }
    }
}

impl PreSerializedValuesTrait for OwnedPreSerializedValues {
    unsafe fn add_value(
        &mut self,
        value_ptr: CsharpValuePtr,
        value_len: usize,
        memory: CsharpMemoryHandler,
    ) {
        let slice = unsafe {
            crate::pre_serialized_values::csharp_value_ptr_as_slice(value_ptr, value_len)
        };
        self.cells.push(BufferOwningCell::Bytes(slice.to_vec()));

        // After copying the bytes into our owned buffer, notify the provided unpin callback
        // that the original handle can be unpinned on the C# side.
        memory.unpin();
    }

    fn add_null(&mut self) {
        self.cells.push(BufferOwningCell::Null);
    }
    fn add_unset(&mut self) {
        self.cells.push(BufferOwningCell::Unset);
    }
    fn len(&self) -> usize {
        self.cells.len()
    }
}

impl SerializeRow for OwnedPreSerializedValues {
    fn serialize(
        &self,
        ctx: &RowSerializationContext<'_>,
        writer: &mut RowWriter,
    ) -> Result<(), SerializationError> {
        crate::pre_serialized_values::serialize_each_cell(
            self,
            ctx,
            writer,
            |cw, cell| match cell {
                BufferOwningCell::Bytes(b) => cw
                    .set_value(b)
                    .map(|_proof| ())
                    .map_err(SerializationError::new),
                BufferOwningCell::Null => {
                    let _ = cw.set_null();
                    Ok(())
                }
                BufferOwningCell::Unset => {
                    let _ = cw.set_unset();
                    Ok(())
                }
            },
        )
    }
    fn is_empty(&self) -> bool {
        self.cells.is_empty()
    }
}

impl HasCells for OwnedPreSerializedValues {
    type Cell = BufferOwningCell;
    fn get_cells(&self) -> &[Self::Cell] {
        &self.cells
    }
}
