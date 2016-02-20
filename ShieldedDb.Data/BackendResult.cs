using System;
using System.Collections.Generic;
using System.Linq;

namespace ShieldedDb.Data
{
    public class BackendResult
    {
        public readonly bool Ok;
        public readonly IEnumerable<DistributedBase> Invalidate;
        public readonly IEnumerable<DistributedBase> Update;

        public BackendResult(bool res)
        {
            Ok = res;
        }

        public BackendResult(IEnumerable<DistributedBase> invalidate, IEnumerable<DistributedBase> update)
        {
            Ok = false;
            Invalidate = invalidate;
            Update = update;
        }

        public BackendResult(IEnumerable<DataOp> failedOps)
        {
            Ok = false;
            Invalidate = failedOps.Select(op => op.Entity);
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

