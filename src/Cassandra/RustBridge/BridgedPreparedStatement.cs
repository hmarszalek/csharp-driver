using System;

using System.Runtime.InteropServices;
using System.Runtime.CompilerServices;
using static Cassandra.RustBridge;
using System.Collections.Generic;

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
            FFIBool isLwt = false;
            unsafe
            {
                RunWithIncrement(handle => prepared_statement_is_lwt(handle, out isLwt, (IntPtr)Globals.ConstructorsPtr));
            }
            return isLwt;
        }

        internal RowSetMetadata ExtractVariablesMetadataFromRust()
        {
            // Query Rust for the number of variable column specs
            var count = GetVariablesColumnSpecsCount();
            if (count == 0)
            {
                return new RowSetMetadata(Array.Empty<CqlColumn>());
            }

            var columns = new CqlColumn[count];
            for (nuint i = 0; i < count; i++)
            {
                columns[i] = new CqlColumn();
            }

            var pk_indexes = new List<int>();
            unsafe
            {
                RunWithIncrement(handle =>
                    prepared_statement_fill_column_specs_metadata(
                        handle,
                        (IntPtr)Unsafe.AsPointer(ref columns),
                        (IntPtr)setColumnMetaPtr,
                        (IntPtr)Unsafe.AsPointer(ref pk_indexes),
                        (IntPtr)AddPkIndexPtr,
                        (IntPtr)Globals.ConstructorsPtr
                    )
                );
            }

            var metadata = new RowSetMetadata(columns, pk_indexes.ToArray());
            return metadata;
        }

        [DllImport("csharp_wrapper", CallingConvention = CallingConvention.Cdecl)]
        unsafe private static extern FFIMaybeException prepared_statement_is_lwt(IntPtr prepared_statement, out FFIBool isLwt, IntPtr constructors);

        [DllImport("csharp_wrapper", CallingConvention = CallingConvention.Cdecl)]
        unsafe private static extern FFIMaybeException prepared_statement_get_variables_column_specs_count(IntPtr prepared_statement, out nuint count, IntPtr constructors);

        [DllImport("csharp_wrapper", CallingConvention = CallingConvention.Cdecl)]
        unsafe private static extern FFIMaybeException prepared_statement_fill_column_specs_metadata(IntPtr prepared_statement, IntPtr columnsPtr, IntPtr metadataSetter, IntPtr pkIndexesPtr, IntPtr addPkIndex, IntPtr constructors);

        private static readonly unsafe delegate* unmanaged[Cdecl]<IntPtr, ushort, FFIMaybeException> AddPkIndexPtr = &AddPkIndex;
        [UnmanagedCallersOnly(CallConvs = new Type[] { typeof(CallConvCdecl) })]
        private static unsafe FFIMaybeException AddPkIndex(
            IntPtr pkIndexesListPtr,
            ushort pkIndex)
        {
            try
            {
                var pkIndexesList = Unsafe.AsRef<List<int>>((void*)pkIndexesListPtr);
                pkIndexesList.Add(pkIndex);
            }
            catch (Exception ex)
            {
                return FFIMaybeException.FromException(ex);
            }

            return FFIMaybeException.Ok();
        }

        private nuint GetVariablesColumnSpecsCount()
        {
            nuint count = 0;
            unsafe
            {
                RunWithIncrement(handle => prepared_statement_get_variables_column_specs_count(handle, out count, (IntPtr)Globals.ConstructorsPtr));
            }
            return count;
        }

        unsafe static readonly delegate* unmanaged[Cdecl]<IntPtr, nuint, FFIString, FFIString, FFIString, byte, IntPtr, byte, FFIMaybeException> setColumnMetaPtr = &BridgedRowSet.SetColumnMeta;
    }
}
