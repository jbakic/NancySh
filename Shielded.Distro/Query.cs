using System;
using System.Linq;
using System.Runtime.Serialization;
using System.Collections.Generic;
using System.Reflection;

namespace Shielded.Distro
{
    [KnownType("GetKnownTypes")]
    public abstract class Query : IEquatable<Query>
    {
        public static readonly QueryAll All = new QueryAll();

        static object _knownTypesLock = new object();
        static IEnumerable<Type> _knownTypes;

        static Query()
        {
            DetectQueryTypes();
        }

        public static void DetectQueryTypes(IEnumerable<Assembly> assemblies = null)
        {
            var qType = typeof(Query);
            var byIdType = typeof(QueryByIds<>);
            lock (_knownTypesLock)
                _knownTypes =
                    (_knownTypes ?? IdTypeContainer.GetTypes().Select(idt => byIdType.MakeGenericType(idt)))
                    .Concat((assemblies ?? Enumerable.Empty<Assembly>())
                        .Concat(AppDomain.CurrentDomain.GetAssemblies())
                        .SelectMany(asm =>
                            asm.GetTypes().Where(t =>
                                t.IsClass && t.IsSubclassOf(qType) && t != byIdType)))
                    .Distinct()
                    .ToArray();
        }

        public static IEnumerable<Type> GetKnownTypes()
        {
            return _knownTypes;
        }

        public abstract bool Check(DistributedBase entity);
        public abstract bool Equals(Query other);

        public override int GetHashCode()
        {
            return _hashCode ?? (_hashCode = GetType().GetHashCode()).Value;
        }

        int? _hashCode;

        public override bool Equals(object obj)
        {
            return Equals(obj as Query);
        }

        public static bool operator==(Query a, Query b)
        {
            if (object.ReferenceEquals(a, null))
                return object.ReferenceEquals(b, null);
            return a.Equals(b);
        }

        public static bool operator!=(Query a, Query b)
        {
            return !(a == b);
        }
    }

    public sealed class QueryAll : Query
    {
        public override bool Check(DistributedBase entity)
        {
            return true;
        }

        public override bool Equals(Query other)
        {
            return other is QueryAll;
        }

        public override int GetHashCode()
        {
            return -1;
        }

        public override string ToString()
        {
            return "QueryAll";
        }
    }

    static class IdTypeContainer
    {
        static IEnumerable<Type> _types;

        public static IEnumerable<Type> GetTypes()
        {
            return _types ?? (_types = Repository.KnownTypes
                .Select(qt => qt.GetProperty("Id"))
                .Where(prop => prop != null)
                .Select(prop => prop.PropertyType)
                .Distinct()
                .ToArray());
        }
    }

    public sealed class QueryByIds<T> : Query
    {
        public T[] Ids;

        public QueryByIds() { }

        public QueryByIds(params T[] ids)
        {
            Ids = ids.OrderBy(id => id).ToArray();
        }

        static EqualityComparer<T> _comp = EqualityComparer<T>.Default;

        public override bool Check(DistributedBase entity)
        {
            var based = entity as DistributedBase<T>;
            return based != null && Ids.Contains(based.Id);
        }

        public override bool Equals(Query other)
        {
            var otherAsId = other as QueryByIds<T>;
            return otherAsId != null && Ids.SequenceEqual(otherAsId.Ids);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                var hash = 17;
                hash = hash * 23 + base.GetHashCode();
                foreach (var id in Ids)
                    hash = hash * 23 + id.GetHashCode();
                return hash;
            }
        }

        public override string ToString()
        {
            return string.Format("QueryByIds<{0}>({1})", typeof(T),
                string.Join(", ", Ids.Select(id => id.ToString())));
        }
    }
}

