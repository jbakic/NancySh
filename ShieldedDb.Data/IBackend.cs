using System;
using System.Linq;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace ShieldedDb.Data
{
    public interface IBackend
    {
        Task<BackendResult> Run(IEnumerable<DataOp> ops);
        IEnumerable<T> LoadAll<T>() where T : DistributedBase, new();
    }

    public class BackendResult
    {
        public readonly bool Ok;
        public readonly IEnumerable<Type> Invalidate;
        public readonly IEnumerable<DistributedBase> Update;

        public BackendResult(bool res)
        {
            Ok = res;
        }

        public BackendResult(IEnumerable<Type> invalidate, IEnumerable<DistributedBase> update)
        {
            Ok = false;
            Invalidate = invalidate;
            Update = update;
        }

        public BackendResult(IEnumerable<DataOp> failedOps)
        {
            Ok = false;
            Invalidate = failedOps.Select(op => op.Entity.GetType());
        }

        public static BackendResult Merge(params BackendResult[] res)
        {
            return res.All(r => r.Ok) ?
                new BackendResult(true) :
                new BackendResult(
                    res.Any(r => r.Invalidate != null) ? res.Where(r => r.Invalidate != null).SelectMany(r => r.Invalidate) : null,
                    res.Any(r => r.Update != null) ? res.Where(r => r.Update != null).SelectMany(r => r.Update) : null);
        }
    }
}

