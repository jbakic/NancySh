using System;
using System.Runtime.Serialization;
using System.Collections.Generic;

namespace ShieldedDb.Data
{
    [KnownType("KnownTypes")]
    public abstract class DistributedBase
    {
        /// <summary>
        /// May never be null.
        /// </summary>
        public virtual object IdValue { get; }
        public virtual int Version { get; set; }

        static IEnumerable<Type> KnownTypes()
        {
            return Repository.KnownTypes;
        }
    }

    public abstract class DistributedBase<TKey> : DistributedBase
    {
        public override object IdValue { get { return Id; } }
        public virtual TKey Id { get; set; }
    }
}

