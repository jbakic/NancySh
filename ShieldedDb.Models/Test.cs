using System;

namespace ShieldedDb.Models
{
    public class Test : IEntity<int>
    {
        public virtual int Id { get; set; }
        public virtual string Val { get; set; }
        public virtual bool Saved { get; set; }
    }
}

