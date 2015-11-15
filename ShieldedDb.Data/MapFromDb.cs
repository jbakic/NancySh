using System;
using System.Collections.Concurrent;
using System.Reflection;
using System.Linq;
using Shielded;
using Shielded.ProxyGen;

namespace ShieldedDb.Data
{
    internal static class MapFromDb
    {
        private static ConcurrentDictionary<Type, PropertyInfo[]> _properties =
            new ConcurrentDictionary<Type, PropertyInfo[]>();

        public static T Map<T>(T source) where T : class, IEntity, new()
        {
            var properties = _properties.GetOrAdd(typeof(T), GetProperties);
            var res = Factory.NewShielded<T>();
            Shield.InTransaction(() => {
                foreach (var prop in properties)
                    prop.SetValue(res, prop.GetValue(source));
                res.Inserted = true;
            });
            return res;
        }

        private static readonly string[] IgnoreFields = { "Inserted" };

        private static PropertyInfo[] GetProperties(Type t)
        {
            return t.GetProperties()
                .Where(p => p.CanRead && p.CanWrite && !IgnoreFields.Contains(p.Name))
                .ToArray();
        }
    }
}

