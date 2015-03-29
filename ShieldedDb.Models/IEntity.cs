using System;

namespace ShieldedDb.Models
{
    public interface IEntity
    {
        bool Saved { get; set; }
    }

    public interface IEntity<TKey> : IEntity
    {
        TKey Id { get; set; }
    }
}

