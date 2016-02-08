using System;
using System.Linq;
using System.Runtime.Serialization;
using System.Collections.Generic;

namespace ShieldedDb.Data
{
    [KnownType("KnownTypes")]
    public abstract class Query : IEquatable<Query>
    {
        public static readonly QueryAll All = new QueryAll();

        private static Dictionary<string, Type> _knownTypes;

        static Query()
        {
            var qType = typeof(Query);
            _knownTypes = AppDomain.CurrentDomain.GetAssemblies()
                .SelectMany(asm =>
                    asm.GetTypes().Where(t =>
                        t.IsClass && t.IsSubclassOf(qType)))
                .ToDictionary(t => t.FullName);
        }

        public static IEnumerable<Type> KnownTypes()
        {
            return _knownTypes.Values;
        }

        public abstract bool Check(DistributedBase entity);
        public abstract bool Equals(Query other);

        public override int GetHashCode()
        {
            throw new NotImplementedException();
        }

        public override bool Equals(object obj)
        {
            var q = obj as Query;
            return q != null && Equals(q);
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

    public class QueryById : Query
    {
        
        
        public override bool Check(DistributedBase entity)
        {
            throw new NotImplementedException();
        }

        public override bool Equals(Query other)
        {
            throw new NotImplementedException();
        }

        public override int GetHashCode()
        {
            return base.GetHashCode();
        }
    }
}

