using System;
using ShieldedDb.Data;

namespace ShieldedDb.Models
{
    public class Test : DistributedBase<int>
    {
        // to save us some boring typing. C# inference is weak.
        public static Accessor<int, Test> Repo = new Accessor<int, Test>();

        public virtual string Val { get; set; }

        public override string ToString()
        {
            return string.Format("[Test: Id={0}, Version={1}, Val={2}]", Id, Version, Val);
        }
    }
}

