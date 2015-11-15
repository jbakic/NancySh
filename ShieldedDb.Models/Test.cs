using System;
using ShieldedDb.Data;

namespace ShieldedDb.Models
{
    public class Test : IEntity<int>
    {
        public virtual int Id { get; set; }
        public virtual string Val { get; set; }
        public virtual bool Inserted { get; set; }

        public override string ToString()
        {
            return string.Format("[Test: Id={0}, Val={1}, Inserted={2}]", Id, Val, Inserted);
        }
    }
}

