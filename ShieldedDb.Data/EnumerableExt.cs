using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace ShieldedDb.Data
{
    public static class EnumerableExt
    {
        public static IEnumerable<TRes> SelectManyParallelSafe<T, TRes>(
            this IEnumerable<T> source, Func<T, IEnumerable<TRes>> selector)
        {
            var temp = source.AsParallel().Select(selector).Where(r => r != null).ToArray();
            if (temp.Length == 0)
                return null;
            return temp.SelectMany(r => r);
        }
    }
}

