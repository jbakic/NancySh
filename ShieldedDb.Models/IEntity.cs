using System;

namespace ShieldedDb.Models
{
    public interface IEntity<TKey>
    {
        TKey Id { get; set; }
        bool Saved { get; set; }
    }
}

