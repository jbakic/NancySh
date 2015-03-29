using System;

namespace ShieldedDb.Models
{
    public class Test : IEntity<int>
    {
        public virtual int Id { get; set; }
        public virtual string Val { get; set; }
        public virtual bool Saved { get; set; }

        public override string ToString()
        {
            return string.Format("[Test: Id={0}, Val={1}, Saved={2}]", Id, Val, Saved);
        }
    }
}

