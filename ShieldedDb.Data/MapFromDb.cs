using System;
using ShieldedDb.Models;
using Shielded.ProxyGen;

namespace ShieldedDb.Data
{
    internal static class MapFromDb
    {
        public static T Map<T>(T source) where T : class, new()
        {
            var testSource = source as Test;
            if (testSource != null)
            {
                var res = Factory.NewShielded<T>();
                var testRes = res as Test;
                testRes.Id = testSource.Id;
                testRes.Val = testSource.Val;
                testRes.Saved = true;
                return res;
            }
            return null;
        }
    }
}

