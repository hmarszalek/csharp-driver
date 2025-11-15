use crate::FfiPtr;
use crate::ffi::{BoxFFI, BridgedBorrowedExclusivePtr, BridgedOwnedExclusivePtr};
use crate::ffi::{FFI, FromBox};
use crate::pre_serialized_values::borrowed_pre_serialized_values::BorrowedPreSerializedValues;
use crate::pre_serialized_values::owned_pre_serialized_values::OwnedPreSerializedValues;
use scylla::serialize::SerializationError;
use scylla::serialize::row::{
    BuiltinSerializationError, BuiltinSerializationErrorKind, BuiltinTypeCheckError,
    BuiltinTypeCheckErrorKind,
};
use scylla::serialize::row::{RowSerializationContext, SerializeRow};
use scylla::serialize::writers::{CellWriter, RowWriter};
use crate::FfiError;

#[derive(Clone, Copy)]
pub enum CsharpValue {}

#[repr(transparent)]
#[derive(Clone, Copy)]
pub struct CsharpValuePtr {
    inner: FfiPtr<'static, CsharpValue>,
}

impl CsharpValuePtr {
    #[inline]
    pub fn as_raw(self) -> *const u8 {
        self.inner
            .ptr
            .map(|p| p.as_ptr() as *const u8)
            .unwrap_or(std::ptr::null())
    }
}

/// # Safety
/// `len` must not exceed the size of the underlying C# buffer.
#[inline]
pub unsafe fn csharp_value_ptr_as_slice(ptr: CsharpValuePtr, len: usize) -> &'static [u8] {
    let raw = ptr.as_raw();
    unsafe { std::slice::from_raw_parts(raw, len) }
}

/// Marker for a C# GCHandle. The concrete target type lives on the C# side;
/// on the Rust side we only care that this is a GCHandle handle.
#[derive(Clone, Copy)]
pub enum CsharpGCHandle {}

#[repr(transparent)]
#[derive(Clone, Copy)]
pub struct CsharpGCHandlePtr {
    inner: FfiPtr<'static, CsharpGCHandle>,
}

impl CsharpGCHandlePtr {}

pub type CSharpUnpinCallback = unsafe extern "C" fn(handle: CsharpGCHandlePtr);

/// Encapsulates the C# GCHandle and its associated unpin callback.
#[derive(Clone, Copy)]
pub(crate) struct CsharpMemoryHandler {
    handle: CsharpGCHandlePtr,
    unpin_cb: CSharpUnpinCallback,
}

impl CsharpMemoryHandler {
    #[inline]
    pub(crate) fn new(handle: CsharpGCHandlePtr, unpin_cb: CSharpUnpinCallback) -> Self {
        Self { handle, unpin_cb }
    }

    #[inline]
    pub(crate) fn unpin(&self) {
        unsafe { (self.unpin_cb)(self.handle) };
    }
}

pub(crate) const MAX_VALUES_LENGTH: usize = u16::MAX as usize;

/// Validates number of values against the provided serialization context.
/// Returns `Ok(())` when counts match and are within limits, otherwise a SerializationError.
pub(crate) fn validate_number_of_columns(
    rust_cols: usize,
    ctx: &RowSerializationContext<'_>,
) -> Result<(), SerializationError> {
    if rust_cols > MAX_VALUES_LENGTH {
        return Err(SerializationError::new(BuiltinSerializationError {
            rust_name: std::any::type_name::<()>(),
            kind: BuiltinSerializationErrorKind::TooManyValues,
        }));
    }
    let cql_cols = ctx.columns().len();
    if rust_cols != cql_cols {
        return Err(SerializationError::new(BuiltinTypeCheckError {
            rust_name: std::any::type_name::<()>(),
            kind: BuiltinTypeCheckErrorKind::WrongColumnCount {
                rust_cols,
                cql_cols,
            },
        }));
    }
    Ok(())
}

/// Iterate over the pre-serialized values' cells and invoke `write_to_cell` for each one.
///
/// `write_to_cell` receives ownership of a `CellWriter` (produced by the provided
/// `RowWriter`) and a reference to the cell value. The closure must finish/consume
/// the `CellWriter` (e.g. call `set_value`, `set_null`, `set_unset` or `into_value_builder().finish()`)
/// and return a `Result<(), SerializationError>`.
pub(crate) fn serialize_each_cell<T, F>(
    pre_serialized_values: &T,
    ctx: &RowSerializationContext<'_>,
    writer: &mut RowWriter,
    mut write_to_cell: F,
) -> Result<(), SerializationError>
where
    T: HasCells + PreSerializedValuesTrait,
    F: FnMut(CellWriter, &T::Cell) -> Result<(), SerializationError>,
{
    validate_number_of_columns(pre_serialized_values.len(), ctx)?;

    // Iterate cells and invoke closure for each one.
    for cell in pre_serialized_values.get_cells().iter() {
        let cw = writer.make_cell_writer();
        write_to_cell(cw, cell)?;
    }

    Ok(())
}

/// Helper trait that exposes access to the concrete cells collection for a
/// particular cell type.
pub(crate) trait HasCells {
    type Cell;
    fn get_cells(&self) -> &[Self::Cell];
}

/// Trait implemented by both the safe (copying) and unsafe (pointer-backed) containers.
pub(crate) trait PreSerializedValuesTrait: SerializeRow + Send + Sync {
    /// Adds a value backed by a C#-owned buffer.
    ///
    /// `value_ptr`/`value_len` describe the buffer; `memory` encapsulates
    /// the GCHandle and its unpin callback on the C# side.
    unsafe fn add_value(
        &mut self,
        value_ptr: CsharpValuePtr,
        value_len: usize,
        csharp_memory_handler: CsharpMemoryHandler,
    );
    fn add_null(&mut self);
    fn add_unset(&mut self);
    fn len(&self) -> usize;
}

pub struct PreSerializedValues {
    inner: Box<dyn PreSerializedValuesTrait>,
}

impl PreSerializedValues {
    fn new_safe() -> Self {
        Self {
            inner: Box::new(OwnedPreSerializedValues::new()),
        }
    }
    fn new_unsafe() -> Self {
        Self {
            inner: Box::new(BorrowedPreSerializedValues::new()),
        }
    }
    unsafe fn add_value(
        &mut self,
        ptr: CsharpValuePtr,
        len: usize,
        csharp_memory_handler: CsharpMemoryHandler,
    ) {
        unsafe {
            self.inner.add_value(ptr, len, csharp_memory_handler);
        }
    }
    fn add_null(&mut self) {
        self.inner.add_null();
    }
    fn add_unset(&mut self) {
        self.inner.add_unset();
    }
}

impl FFI for PreSerializedValues {
    type Origin = FromBox;
}

impl SerializeRow for PreSerializedValues {
    fn serialize(
        &self,
        ctx: &RowSerializationContext<'_>,
        writer: &mut RowWriter,
    ) -> Result<(), SerializationError> {
        self.inner.serialize(ctx, writer)
    }
    fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }
}

// ===== FFI exported functions =====
#[unsafe(no_mangle)]
pub extern "C" fn pre_serialized_values_new() -> BridgedOwnedExclusivePtr<PreSerializedValues> {
    BoxFFI::into_ptr(Box::new(PreSerializedValues::new_safe()))
}

#[unsafe(no_mangle)]
pub extern "C" fn pre_serialized_values_borrowed_new()
-> BridgedOwnedExclusivePtr<PreSerializedValues> {
    BoxFFI::into_ptr(Box::new(PreSerializedValues::new_unsafe()))
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn pre_serialized_values_add_value(
    builder_ptr: BridgedBorrowedExclusivePtr<'_, PreSerializedValues>,
    value_ptr: CsharpValuePtr,
    value_len: usize,
    handle: CsharpGCHandlePtr,
    unpin_cb: CSharpUnpinCallback,
) -> FfiError {
    if let Some(builder) = BoxFFI::as_mut_ref(builder_ptr) {
        unsafe {
            builder.add_value(
                value_ptr,
                value_len,
                CsharpMemoryHandler::new(handle, unpin_cb),
            );
        }
        FfiError::ok()
    } else {
        // Invalid pointer from C# side
        FfiError::new(1, "invalid PreSerializedValues pointer in pre_serialized_values_add_value")
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn pre_serialized_values_add_null(
    builder_ptr: BridgedBorrowedExclusivePtr<'_, PreSerializedValues>,
) -> FfiError {
    if let Some(builder) = BoxFFI::as_mut_ref(builder_ptr) {
        builder.add_null();
        FfiError::ok()
    } else {
        FfiError::new(1, "invalid PreSerializedValues pointer in pre_serialized_values_add_null")
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn pre_serialized_values_add_unset(
    builder_ptr: BridgedBorrowedExclusivePtr<'_, PreSerializedValues>,
) -> FfiError {
    if let Some(builder) = BoxFFI::as_mut_ref(builder_ptr) {
        builder.add_unset();
        FfiError::ok()
    } else {
        FfiError::new(1, "invalid PreSerializedValues pointer in pre_serialized_values_add_unset")
    }
}
