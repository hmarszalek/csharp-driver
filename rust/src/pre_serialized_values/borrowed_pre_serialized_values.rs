use crate::FfiPtr;
use crate::pre_serialized_values::{
    CsharpGCHandlePtr, CsharpMemoryHandler, CsharpValuePtr, HasCells, PreSerializedValuesTrait,
};
use scylla::serialize::SerializationError;
use scylla::serialize::row::{RowSerializationContext, SerializeRow};
use scylla::serialize::writers::RowWriter;
use std::marker::PhantomData;
use std::ptr::NonNull;

/// The underlying memory is owned and pinned by C# and must remain valid for the
/// lifetime of the enclosing pre-serialized values object.
#[derive(Clone, Copy)]
pub(crate) struct BorrowedBytesPtr<'a>(FfiPtr<'a, u8>);

/// Safety: the underlying pointer refers to an immutable, pinned C# buffer.
/// The GCHandle guarantees that the buffer will not move and remains valid
/// until the corresponding unpin callback is invoked. We never mutate through
/// this pointer, only create shared slices.
unsafe impl<'a> Send for BorrowedBytesPtr<'a> {}
unsafe impl<'a> Sync for BorrowedBytesPtr<'a> {}

impl<'a> BorrowedBytesPtr<'a> {
    /// # Safety
    /// `len` must not exceed the size of the underlying C# buffer.
    pub unsafe fn from_raw(ptr: CsharpValuePtr) -> Self {
        let nonnull = NonNull::new(ptr.as_raw() as *mut u8)
            .expect("null buffer pointer for BorrowedBytesPtr");
        BorrowedBytesPtr(FfiPtr {
            ptr: Some(nonnull),
            _phantom: PhantomData,
        })
    }

    /// # Safety
    /// Caller must ensure that `len` does not exceed the actual buffer length
    /// guaranteed by the C# side and the associated GCHandle pin.
    pub unsafe fn as_slice(&self, len: usize) -> &[u8] {
        let ptr = self.0.ptr.unwrap().as_ptr();
        unsafe { std::slice::from_raw_parts(ptr, len) }
    }
}

// --- Cell Implementation ---

pub(crate) enum BufferBorrowingCell<'a> {
    Bytes {
        data: BorrowedBytesPtr<'a>,
        len: usize,
        handler: CsharpMemoryHandler,
    },
    Null,
    Unset,
}

impl<'a> BufferBorrowingCell<'a> {
    fn unpin_buffer_if_needed(&self) {
        if let BufferBorrowingCell::Bytes { handler, .. } = self {
            handler.unpin();
        }
    }
}

impl<'a> Drop for BufferBorrowingCell<'a> {
    fn drop(&mut self) {
        self.unpin_buffer_if_needed()
    }
}

pub struct BorrowedPreSerializedValues<'a> {
    cells: Vec<BufferBorrowingCell<'a>>,
}

impl<'a> BorrowedPreSerializedValues<'a> {
    pub fn new() -> Self {
        Self { cells: Vec::new() }
    }
}

/// Safety: CsharpGCHandlePtr only refers to a GCHandle handle on the C# side. It's the C#
/// side's responsibility to ensure that the handle remains valid and pinned while in use on
/// the Rust side.
unsafe impl Send for CsharpGCHandlePtr {}
unsafe impl Sync for CsharpGCHandlePtr {}

impl<'a> PreSerializedValuesTrait for BorrowedPreSerializedValues<'a> {
    unsafe fn add_value(
        &mut self,
        value_ptr: CsharpValuePtr,
        value_len: usize,
        csharp_memory_handler: CsharpMemoryHandler,
    ) {
        self.cells.push(BufferBorrowingCell::Bytes {
            data: unsafe { BorrowedBytesPtr::from_raw(value_ptr) },
            len: value_len,
            handler: csharp_memory_handler,
        });
    }

    fn add_null(&mut self) {
        self.cells.push(BufferBorrowingCell::Null);
    }
    fn add_unset(&mut self) {
        self.cells.push(BufferBorrowingCell::Unset);
    }
    fn len(&self) -> usize {
        self.cells.len()
    }
}

impl<'a> SerializeRow for BorrowedPreSerializedValues<'a> {
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
                BufferBorrowingCell::Bytes { data, len, .. } => {
                    let slice = unsafe { data.as_slice(*len) };
                    cw.set_value(slice)
                        .map(|_proof| ())
                        .map_err(SerializationError::new)
                }
                BufferBorrowingCell::Null => {
                    let _ = cw.set_null();
                    Ok(())
                }
                BufferBorrowingCell::Unset => {
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

impl<'a> HasCells for BorrowedPreSerializedValues<'a> {
    type Cell = BufferBorrowingCell<'a>;
    fn get_cells(&self) -> &[Self::Cell] {
        &self.cells
    }
}
