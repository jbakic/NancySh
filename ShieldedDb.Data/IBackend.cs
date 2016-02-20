using System;
using System.Linq;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace ShieldedDb.Data
{
    public interface IBackend
    {
        Task<BackendResult> Run(IEnumerable<DataOp> ops);
        QueryResult<T> Query<T>(Query query) where T : DistributedBase, new();
    }
}

