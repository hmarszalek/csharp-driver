pub mod ffi;
mod logging;
mod pre_serialized_values;
mod prepared_statement;
mod row_set;
mod session;
mod task;

use std::ffi::{CStr, CString, c_char};
use std::marker::PhantomData;
use std::ptr::NonNull;

#[repr(transparent)]
#[derive(Clone, Copy)]
pub struct FfiPtr<'a, T: Sized> {
    ptr: Option<NonNull<T>>,
    _phantom: PhantomData<&'a ()>,
}

type CSharpStr<'a> = FfiPtr<'a, c_char>;
impl<'a> CSharpStr<'a> {
    fn as_cstr(&self) -> Option<&CStr> {
        self.ptr.map(|ptr| unsafe { CStr::from_ptr(ptr.as_ptr()) })
    }
}

/// Simple error struct that can be returned over FFI.
#[repr(C)]
pub struct FfiError {
    /// Numeric error code; 0 means success.
    code: i32,
    /// Pointer to a NUL-terminated UTF-8 message allocated by Rust.
    /// The receiver is responsible for eventually freeing it with
    /// `ffi_error_free_message`.
    message: *mut c_char,
}

impl FfiError {
    pub fn ok() -> Self {
        FfiError {
            code: 0,
            message: std::ptr::null_mut(),
        }
    }

    pub fn new(code: i32, msg: &str) -> Self {
        // Allocate a CString so C# can read and later free it.
        let cstring = CString::new(msg).unwrap_or_else(|_| CString::new("invalid utf8 in error").unwrap());
        let ptr = cstring.into_raw();
        FfiError { code, message: ptr }
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn ffi_error_free_message(msg: *mut c_char) {
    if msg.is_null() {
        return;
    }
    unsafe {
        // SAFETY: `msg` must have been allocated from a `CString::into_raw`.
        let _ = CString::from_raw(msg);
    }
}
