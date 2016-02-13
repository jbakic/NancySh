﻿using System;
using System.Linq;
using System.Runtime.Serialization;
using System.Collections.Generic;
using System.Reflection;

namespace ShieldedDb.Data
{
    [KnownType("GetKnownTypes")]
    public abstract class Query : IEquatable<Query>
    {
        public static readonly QueryAll All = new QueryAll();

        private static IEnumerable<Type> _knownTypes;

        static Query()
        {
            DetectQueryTypes();
        }

        public static void DetectQueryTypes(IEnumerable<Assembly> assemblies = null)
        {
            var qType = typeof(Query);
            var byIdType = typeof(QueryById<>);
            _knownTypes = (assemblies ?? AppDomain.CurrentDomain.GetAssemblies())
                .SelectMany(asm =>
                    asm.GetTypes().Where(t =>
                        t.IsClass && t.IsSubclassOf(qType) && t != byIdType))
                .Concat(IdTypeContainer.GetTypes().Select(idt => byIdType.MakeGenericType(idt)))
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
            throw new NotImplementedException();
        }

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

    public sealed class QueryById<T> : Query
    {
        public T Id;

        public QueryById() { }

        public QueryById(T id)
        {
            Id = id;
        }

        static EqualityComparer<T> _comp = EqualityComparer<T>.Default;

        public override bool Check(DistributedBase entity)
        {
            var based = entity as DistributedBase<T>;
            return based != null && _comp.Equals(based.Id, Id);
        }

        public override bool Equals(Query other)
        {
            var otherAsId = other as QueryById<T>;
            return otherAsId != null && _comp.Equals(otherAsId.Id, Id);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                var hash = 17;
                hash = hash * 23 + GetType().GetHashCode();
                hash = hash * 23 + Id.GetHashCode();
                return hash;
            }
        }
    }
}

