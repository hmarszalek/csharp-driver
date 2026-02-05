use super::pre_serialized_values::PreSerializedValues;
use crate::error_conversion::FfiException;
use crate::ffi::{BoxFFI, BridgedBorrowedExclusivePtr, BridgedOwnedExclusivePtr, FFIByteSlice};
use crate::task::ExceptionConstructors;
// TODO: consider moving to pre_serialized_values/pre_serialized_values.rs

#[unsafe(no_mangle)]
pub extern "C" fn pre_serialized_values_new() -> BridgedOwnedExclusivePtr<PreSerializedValues> {
    BoxFFI::into_ptr(Box::new(PreSerializedValues::new()))
}

/// Adds a pre-serialized value from a C#-owned buffer to the builder.
///
/// # Safety
/// `value_ptr` and the data it points to must remain valid for the duration of this call.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn pre_serialized_values_add_value(
    values_ptr: BridgedBorrowedExclusivePtr<'_, PreSerializedValues>,
    value: FFIByteSlice<'_>,
    constructors: &'static ExceptionConstructors,
) -> FfiException {
    let Some(values) = BoxFFI::as_mut_ref(values_ptr) else {
        panic!("invalid PreSerializedValues pointer in pre_serialized_values_add_value");
    };
    match values.add_value(value) {
        Ok(()) => FfiException::ok(),
        Err(e) => FfiException::from_error(e, constructors),
    }
}

/// Adds a null cell to the builder.
#[unsafe(no_mangle)]
pub extern "C" fn pre_serialized_values_add_null(
    values_ptr: BridgedBorrowedExclusivePtr<'_, PreSerializedValues>,
    constructors: &'static ExceptionConstructors,
) -> FfiException {
    let Some(values) = BoxFFI::as_mut_ref(values_ptr) else {
        panic!("invalid PreSerializedValues pointer in pre_serialized_values_add_null");
    };
    match values.add_null() {
        Ok(()) => FfiException::ok(),
        Err(e) => FfiException::from_error(e, constructors),
    }
}

/// Adds an unset cell to the builder.
#[unsafe(no_mangle)]
pub extern "C" fn pre_serialized_values_add_unset(
    values_ptr: BridgedBorrowedExclusivePtr<'_, PreSerializedValues>,
    constructors: &'static ExceptionConstructors,
) -> FfiException {
    let Some(values) = BoxFFI::as_mut_ref(values_ptr) else {
        panic!("invalid PreSerializedValues pointer in pre_serialized_values_add_unset");
    };
    match values.add_unset() {
        Ok(()) => FfiException::ok(),
        Err(e) => FfiException::from_error(e, constructors),
    }
}

/// Frees the PreSerializedValues if it was not consumed by a query.
#[unsafe(no_mangle)]
pub extern "C" fn pre_serialized_values_free(
    values_ptr: BridgedOwnedExclusivePtr<PreSerializedValues>,
) {
    // Simply drop the Box<PreSerializedValues>
    let _ = BoxFFI::from_ptr(values_ptr);
}
