use super::csharp_memory::{CsharpSerializedValue, CsharpValuePtr};
use super::pre_serialized_values::PreSerializedValues;
use crate::FfiError;
use crate::ffi::{BoxFFI, BridgedBorrowedExclusivePtr, BridgedOwnedExclusivePtr};

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
    value_ptr: CsharpValuePtr,
    value_len: usize,
) -> FfiError {
    // Validate and obtain mutable reference to PreSerializedValues
    let values = match BoxFFI::as_mut_ref(values_ptr) {
        Some(v) => v,
        None => {
            return crate::ffi_error_from_err(
                "invalid PreSerializedValues pointer in pre_serialized_values_add_value",
            );
        }
    };

    // Build the C# serialized value and add it
    let value = CsharpSerializedValue::new(value_ptr, value_len);
    if let Err(e) = values.add_value(value) {
        return crate::ffi_error_from_err(format!("failed to add value: {}", e));
    }

    FfiError::ok()
}

/// Adds a null cell to the builder.
#[unsafe(no_mangle)]
pub extern "C" fn pre_serialized_values_add_null(
    values_ptr: BridgedBorrowedExclusivePtr<'_, PreSerializedValues>,
) -> FfiError {
    let values = match BoxFFI::as_mut_ref(values_ptr) {
        Some(v) => v,
        None => {
            return crate::ffi_error_from_err(
                "invalid PreSerializedValues pointer in pre_serialized_values_add_null",
            );
        }
    };

    if let Err(e) = values.add_null() {
        return crate::ffi_error_from_err(format!("failed to add null: {}", e));
    }

    FfiError::ok()
}

/// Adds an unset cell to the builder.
#[unsafe(no_mangle)]
pub extern "C" fn pre_serialized_values_add_unset(
    values_ptr: BridgedBorrowedExclusivePtr<'_, PreSerializedValues>,
) -> FfiError {
    let values = match BoxFFI::as_mut_ref(values_ptr) {
        Some(v) => v,
        None => {
            return crate::ffi_error_from_err(
                "invalid PreSerializedValues pointer in pre_serialized_values_add_unset",
            );
        }
    };

    if let Err(e) = values.add_unset() {
        return crate::ffi_error_from_err(format!("failed to add unset: {}", e));
    }

    FfiError::ok()
}

/// Frees the PreSerializedValues if it was not consumed by a query.
#[unsafe(no_mangle)]
pub extern "C" fn pre_serialized_values_free(
    values_ptr: BridgedOwnedExclusivePtr<PreSerializedValues>,
) {
    // Simply drop the Box<PreSerializedValues>
    let _ = BoxFFI::from_ptr(values_ptr);
}
