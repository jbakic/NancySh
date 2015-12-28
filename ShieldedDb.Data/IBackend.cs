using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace ShieldedDb.Data
{
    public interface IBackend
    {
        Task<bool> Run(IEnumerable<DataOp> ops);
        T[] LoadAll<T>() where T : DistributedBase, new();
    }
}

