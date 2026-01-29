using System;

using System.Runtime.InteropServices;
using System.Runtime.CompilerServices;

namespace Cassandra
{
    /// <summary>
    /// Bridges a Rust-owned prepared statement resource to C# via SafeHandle.
    /// Inherits destructor and handle management from RustResource.
    /// </summary>
    internal sealed class BridgedPreparedStatement : RustResource
    {
        internal BridgedPreparedStatement(ManuallyDestructible mdPrepared) : base(mdPrepared)
        {
        }

        internal bool IsLwt()
        {
            bool isLwt = false;
            RunWithIncrement(handle => prepared_statement_is_lwt(handle, out isLwt));
            return isLwt;
        }

        internal RowSetMetadata ExtractVariablesFromRust()
        {
            // Query Rust for the number of column specs
            var count = GetColumnSpecsCount();
            if (count <= 0)
            {
                return new RowSetMetadata();
            }

            var columns = new CqlColumn[count];
            for (nuint i = 0; i < count; i++)
            {
                columns[i] = new CqlColumn();
            }

            unsafe
            {
                void* columnsPtr = Unsafe.AsPointer(ref columns);
                FillVariablesMetadata((IntPtr)columnsPtr, (IntPtr)setColumnMetaPtr);
            }
            
            var metadata = new RowSetMetadata
            {
                Columns = columns
            };

            return metadata;
        }

        [DllImport("csharp_wrapper", CallingConvention = CallingConvention.Cdecl)]
        unsafe private static extern RustBridge.FfiException prepared_statement_is_lwt(IntPtr prepared_statement, [MarshalAs(UnmanagedType.U1)] out bool isLwt);

        [DllImport("csharp_wrapper", CallingConvention = CallingConvention.Cdecl)]
        unsafe private static extern RustBridge.FfiException prepared_statement_get_column_specs_count(IntPtr prepared_statement, out nuint count);

        [DllImport("csharp_wrapper", CallingConvention = CallingConvention.Cdecl)]
        unsafe private static extern RustBridge.FfiException prepared_statement_fill_column_specs_metadata(IntPtr prepared_statement, IntPtr columnsPtr, IntPtr metadataSetter);

        private nuint GetColumnSpecsCount()
        {
            nuint count = 0;
            RunWithIncrement(handle => prepared_statement_get_column_specs_count(handle, out count));
            return count;
        }

        private void FillVariablesMetadata(IntPtr columnsPtr, IntPtr metadataSetter)
        {
            RunWithIncrement(handle => prepared_statement_fill_column_specs_metadata(handle, columnsPtr, metadataSetter));
        }

        unsafe static readonly delegate* unmanaged[Cdecl]<IntPtr, nuint, FFIString, FFIString, FFIString, byte, IntPtr, byte, RustBridge.FfiException> setColumnMetaPtr = &BridgedRowSet.SetColumnMeta;
    }
}
