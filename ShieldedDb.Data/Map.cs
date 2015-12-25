using System;
using System.Collections.Concurrent;
using System.Reflection;
using System.Linq;
using Shielded;
using Shielded.ProxyGen;

namespace ShieldedDb.Data
{
    /// <summary>
    /// Maps from and to shielded objects.
    /// </summary>
    public static class Map
    {
        private static ConcurrentDictionary<Type, PropertyInfo[]> _properties =
            new ConcurrentDictionary<Type, PropertyInfo[]>();

        /// <summary>
        /// If shielded, returns the same object.
        /// </summary>
        public static T ToShielded<T>(T source) where T : class, IDistributed, new()
        {
            var type = source.GetType();
            if (Factory.IsProxy(type))
                return source;
            var res = Factory.NewShielded<T>();
            Copy(typeof(T), source, res);
            return res;
        }

        public static T NonShieldedClone<T>(T source) where T : IDistributed
        {
            var entityType = source.GetType();
            if (Factory.IsProxy(entityType))
                entityType = entityType.BaseType;
            var res = Activator.CreateInstance(entityType);
            Copy(entityType, source, res);
            return (T)res;
        }

        private static void Copy(Type t, object source, object dest)
        {
            var properties = _properties.GetOrAdd(t, GetProperties);
            Shield.InTransaction(() => {
                foreach (var prop in properties)
                    prop.SetValue(dest, prop.GetValue(source));
            });
        }

        private static PropertyInfo[] GetProperties(Type t)
        {
            return t.GetProperties()
                .Where(p => p.CanRead && p.CanWrite)
                .ToArray();
        }
    }
}

