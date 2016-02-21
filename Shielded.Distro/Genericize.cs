using System;
using System.Collections.Concurrent;
using System.Reflection;

namespace Shielded.Distro
{
    public class Genericize
    {
        readonly ConcurrentDictionary<Type, MethodInfo> _methodForType = new ConcurrentDictionary<Type, MethodInfo>();
        readonly Func<Type, MethodInfo> GetImportForType;

        public Genericize(Func<Type, MethodInfo> factory)
        {
            GetImportForType = factory;
        }

        public MethodInfo Get(Type t)
        {
            return _methodForType.GetOrAdd(t, GetImportForType);
        }
    }
}

