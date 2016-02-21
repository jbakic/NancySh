using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Shielded.Distro
{
    public static class QueryResultExt
    {
        public static QueryResult<TRes> QueryParallelSafe<T, TRes>(
            this IEnumerable<T> source, Func<T, QueryResult<TRes>> selector) where TRes : DistributedBase
        {
            var temp = source.AsParallel().Select(selector).Where(r => r != null).ToArray();
            if (temp.Length == 0)
                return null;
            return QueryResult<TRes>.Merge(temp);
        }
    }
}

