using System;

namespace ShieldedDb.Models
{
    public interface IEntity { }

    public interface IEntity<TKey> : IEntity
    {
        TKey Id { get; set; }
        bool Saved { get; set; }
    }
}

