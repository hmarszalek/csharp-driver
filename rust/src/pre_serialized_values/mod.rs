#[path = "borrowed_pre_serialized_values.rs"]
pub(crate) mod borrowed_pre_serialized_values;
#[path = "owned_pre_serialized_values.rs"]
pub(crate) mod owned_pre_serialized_values;
#[path = "pre_serialized_values.rs"]
pub(crate) mod pre_serialized_values;

pub(crate) use pre_serialized_values::{
    CsharpGCHandlePtr, CsharpMemoryHandler, CsharpValuePtr, HasCells, PreSerializedValues,
    PreSerializedValuesTrait, csharp_value_ptr_as_slice, serialize_each_cell,
};
