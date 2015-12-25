using System;

namespace ShieldedDb.Data
{
    public interface IDistributed
    {
        object IdValue { get; }
    }

    public abstract class DistributedBase<TKey> : IDistributed
    {
        public virtual TKey Id { get; set; }
        object IDistributed.IdValue { get { return Id; } }
    }
}

