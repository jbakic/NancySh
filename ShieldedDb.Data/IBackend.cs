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

    public class QueryResult<T> where T : DistributedBase
    {
        public readonly bool QueryOwned;
        public readonly IEnumerable<T> Result;
        public readonly IEnumerable<T> Owned; // if QueryOwned, == Result

        public QueryResult(bool queryOwned, IEnumerable<T> res = null, IEnumerable<T> owned = null)
        {
            QueryOwned = queryOwned;
            Result = res ?? Enumerable.Empty<T>();
            Owned = QueryOwned ? Result : (owned ?? Enumerable.Empty<T>());
        }

        public static QueryResult<T> Merge(IEnumerable<QueryResult<T>> res)
        {
            return new QueryResult<T>(
                res.Any(r => r.QueryOwned),
                res.SelectMany(r => r.Result),
                res.SelectMany(r => r.Owned));
        }
    }
}

