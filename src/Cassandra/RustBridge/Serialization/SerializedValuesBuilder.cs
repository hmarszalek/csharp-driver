using System;
using System.Collections.Generic;
using Cassandra.Serialization;

namespace Cassandra.RustBridge.Serialization
{
    internal sealed class SerializedValuesBuilder
    {
        private readonly ISerializer _serializer;
        private readonly List<object> _values = [];

        internal SerializedValuesBuilder()
        {
            _serializer = SerializerManager.Default.GetCurrentSerializer();
        }
        
        /// <summary>
        /// Adds a value to this build. Serialization rules:
        ///  - null: stored as null (NULL marker)
        ///  - Unset.Value: stored as Unset.Value (UNSET marker)
        ///  - byte[]: assumed already serialized
        /// </summary>
        private void Add(object value)
        {
            if (value == null)
            {
                _values.Add(null);
                return;
            }
            if (ReferenceEquals(value, Unset.Value))
            {
                _values.Add(Unset.Value);
                return;
            }
            var buf = _serializer.Serialize(value);
            _values.Add(buf);
        }

        internal SerializedValuesBuilder AddMany(IEnumerable<object> values)
        {
            ArgumentNullException.ThrowIfNull(values);
            foreach (var v in values)
            {
                Add(v);
            }
            return this;
        }

        internal ISerializedValues Build()
        {
            return BorrowedSerializedValues.Build(_values);
        }
    }
}
