using System;
using System.Collections.Generic;
using System.Linq;

namespace ShieldedDb.Data
{
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

        public static QueryResult<T> Merge(params QueryResult<T>[] res)
        {
            return new QueryResult<T>(
                res.Any(r => r.QueryOwned),
                res.SelectMany(r => r.Result),
                res.SelectMany(r => r.Owned));
        }
    }
}

