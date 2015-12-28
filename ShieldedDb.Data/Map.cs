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
        /// If shielded, returns the same object. If not, makes a shielded clone
        /// based on the object's actual type, not based on the generic type argument.
        /// </summary>
        public static T ToShielded<T>(T source) where T : DistributedBase
        {
            var type = source.GetType();
            if (Factory.IsProxy(type))
                return source;
            var res = Activator.CreateInstance(Factory.ShieldedType(type));
            Copy(type, source, res);
            return (T)res;
        }

        /// <summary>
        /// Always produces a new object, based on the input object's actual
        /// type (or, base type, if the object is a proxy).
        /// </summary>
        public static T NonShieldedClone<T>(T source) where T : DistributedBase
        {
            var entityType = source.GetType();
            if (Factory.IsProxy(entityType))
                entityType = entityType.BaseType;
            var res = Activator.CreateInstance(entityType);
            Copy(entityType, source, res);
            return (T)res;
        }

        internal static void Copy(Type t, object source, object dest)
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

