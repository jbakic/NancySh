using System;
using System.Collections.Generic;
using System.Linq;
using Shielded;
using Shielded.ProxyGen;
using System.Threading;

namespace ShieldedDb.Data
{
    /// <summary>
    /// Global repo of all live entities.
    /// </summary>
    static class EntityDictionary
    {
        private class TypeDict
        {
            public readonly ShieldedDict<object, IDistributed> Entities;
            public readonly bool HasAll;

            public TypeDict(ShieldedDict<object, IDistributed> dict, bool hasAll)
            {
                Entities = dict ?? new ShieldedDict<object, IDistributed>();
                HasAll = hasAll;
            }
        }

        static readonly ShieldedDictNc<Type, TypeDict> _typeDicts =
            new ShieldedDictNc<Type, TypeDict>();

        static Type Normalize(Type source)
        {
            return Factory.IsProxy(source) ? source.BaseType : source;
        }

        static TypeDict GetTypeDict(Type type)
        {
            type = Normalize(type);
            TypeDict dict;
            if (!_typeDicts.TryGetValue(type, out dict))
            {
                dict = new TypeDict(dict != null ? dict.Entities : null, false);
                _typeDicts[type] = dict;
            }
            return dict;
        }

        public static bool IsTracked(IDistributed entity)
        {
            var type = Normalize(entity.GetType());
            TypeDict dict;
            return _typeDicts.TryGetValue(type, out dict) && dict.Entities.ContainsKey(entity.IdValue);
        }

        public static TRes Query<T, TRes>(Func<ShieldedDict<object, IDistributed>, TRes> query,
            Func<IEnumerable<T>> allGetter = null, int allGetterTimeoutMs = Timeout.Infinite) where T : IDistributed
        {
            return Shield.InTransaction(() => {
                var dict = GetTypeDict(typeof(T));
                if (dict.HasAll || allGetter == null)
                    return query(dict.Entities);

                // we'll need to load entities...
                Shield.SideEffect(null, () => {
                    // we run a dummy trans just to lock the key...
                    using (var cont = Shield.RunToCommit(allGetterTimeoutMs, () => {
                        _typeDicts[typeof(T)] = dict = GetTypeDict(typeof(T));
                    }))
                    {
                        // if someone beat us to it...
                        if (dict.HasAll)
                            return;
                        Import((IEnumerable<IDistributed>)allGetter(), dict.Entities);
                        cont.InContext(() => _typeDicts[typeof(T)] = new TypeDict(dict.Entities, true));
                        cont.Commit();
                    }
                });
                Shield.Rollback();
                return default(TRes);
            });
        }

        public static T Add<T>(T entity) where T : IDistributed
        {
            var dict = GetTypeDict(entity.GetType());
            if (dict.Entities.ContainsKey(entity.IdValue))
                throw new InvalidOperationException("Entity of the same type with same ID already known.");
            var res = Map.ToShielded(entity);
            dict.Entities.Add(entity.IdValue, res);
            return res;
        }

        public static void Remove(IDistributed entity)
        {
            var dict = GetTypeDict(entity.GetType());
            if (!dict.Entities.Remove(entity.IdValue) && dict.HasAll)
                throw new KeyNotFoundException();
        }

        [ThreadStatic]
        static bool _importTransaction;

        public static bool IsImporting
        {
            get
            {
                return _importTransaction;
            }
        }

        public static void Import(IEnumerable<IDistributed> dtos)
        {
            Import(dtos, null);
        }

        static void Import(IEnumerable<IDistributed> dtos, ShieldedDict<object, IDistributed> specificDict)
        {
            if (Shield.IsInTransaction)
                throw new InvalidOperationException("Import can not be a part of a bigger transaction.");
            try
            {
                _importTransaction = true;
                Shield.InTransaction(() => {
                    foreach (var dto in dtos)
                    {
                        var dict = specificDict ?? GetTypeDict(dto.GetType()).Entities;
                        IDistributed old;
                        if (dict.TryGetValue(dto.IdValue, out old))
                            Merge(old, dto);
                        else
                            dict[dto.IdValue] = Map.ToShielded(dto);
                    }
                });
            }
            finally
            {
                _importTransaction = false;
            }
        }

        static void Merge(IDistributed old, IDistributed dto)
        {
            // TODO: entities should get versions. only if the dto has a higher version than
            // the old entity, then it's data should be copied like this.
            Map.Copy(old.GetType().BaseType, dto, old);
        }
    }
}

