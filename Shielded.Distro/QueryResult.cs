using System;
using System.Collections.Generic;
using System.Linq;

namespace Shielded.Distro
{
    public class QueryResult<T> where T : DistributedBase
    {
        public readonly bool QueryOwned;
        public readonly IEnumerable<T> Result;

        public QueryResult(bool queryOwned, IEnumerable<T> res = null)
        {
            QueryOwned = queryOwned;
            Result = res ?? Enumerable.Empty<T>();
        }

        public static QueryResult<T> Merge(params QueryResult<T>[] res)
        {
            return new QueryResult<T>(
                res.Any(r => r.QueryOwned),
                res.SelectMany(r => r.Result));
        }
    }
}

