using System;

namespace ShieldedDb.Data
{
    public interface IEntity
    {
        bool Inserted { get; set; }
    }

    public interface IEntity<TKey> : IEntity
    {
        TKey Id { get; set; }
    }
}

