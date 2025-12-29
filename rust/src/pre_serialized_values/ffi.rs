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
    let values = crate::ffi_try!(
        BoxFFI::as_mut_ref(values_ptr).ok_or(
            "invalid PreSerializedValues pointer in pre_serialized_values_add_value".to_string()
        ),
        1,
        "invalid PreSerializedValues pointer in pre_serialized_values_add_value: {}"
    );

    // Build the C# serialized value and add it
    let value = CsharpSerializedValue::new(value_ptr, value_len);
    crate::ffi_try!(
        unsafe { values.add_value(value) }.map_err(|e| format!("failed to add value: {}", e)),
        1,
        "failed to add value: {}"
    );

    FfiError::ok()
}

/// Adds a null cell to the builder.
#[unsafe(no_mangle)]
pub extern "C" fn pre_serialized_values_add_null(
    values_ptr: BridgedBorrowedExclusivePtr<'_, PreSerializedValues>,
) -> FfiError {
    let values = crate::ffi_try!(
        BoxFFI::as_mut_ref(values_ptr).ok_or(
            "invalid PreSerializedValues pointer in pre_serialized_values_add_null".to_string()
        ),
        1,
        "invalid PreSerializedValues pointer in pre_serialized_values_add_null: {}"
    );
    crate::ffi_try!(
        values
            .add_null()
            .map_err(|e| format!("failed to add null: {}", e)),
        1,
        "failed to add null: {}"
    );
    FfiError::ok()
}

/// Adds an unset cell to the builder.
#[unsafe(no_mangle)]
pub extern "C" fn pre_serialized_values_add_unset(
    values_ptr: BridgedBorrowedExclusivePtr<'_, PreSerializedValues>,
) -> FfiError {
    let values = crate::ffi_try!(
        BoxFFI::as_mut_ref(values_ptr).ok_or(
            "invalid PreSerializedValues pointer in pre_serialized_values_add_unset".to_string()
        ),
        1,
        "invalid PreSerializedValues pointer in pre_serialized_values_add_unset: {}"
    );
    crate::ffi_try!(
        values
            .add_unset()
            .map_err(|e| format!("failed to add unset: {}", e)),
        1,
        "failed to add unset: {}"
    );
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
