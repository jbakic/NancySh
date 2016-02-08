using System;
using System.Collections.Generic;
using System.Linq;
using Shielded;
using Shielded.ProxyGen;
using System.Threading;
using System.Reflection;
using System.Collections.Concurrent;

namespace ShieldedDb.Data
{
    public delegate TRes QueryFunc<TKey, T, TRes>(IDictionary<TKey, T> dict) where T : DistributedBase<TKey>, new();

    /// <summary>
    /// Global repo of all live entities.
    /// </summary>
    static class EntityDictionary
    {
        struct TypeDict<TKey, T> where T : DistributedBase<TKey>
        {
            public ShieldedDict<TKey, T> Entities;
            public Tuple<Query, IDictionary<TKey, T>>[] CachedQueries;

            public static Shielded<TypeDict<TKey, T>> Ref = new Shielded<TypeDict<TKey, T>>(
                new TypeDict<TKey, T> {
                    Entities = new ShieldedDict<TKey, T>(),
                    CachedQueries = new Tuple<Query, IDictionary<TKey, T>>[0],
                });
        }

        public static TRes Query<TKey, T, TRes>(QueryFunc<TKey, T, TRes> queryFunc, Query query) where T : DistributedBase<TKey>, new()
        {
            return Shield.InTransaction(() => {
                var dictOuter = TypeDict<TKey, T>.Ref.Value;
                if (query == null)
                    return queryFunc(dictOuter.Entities);
                Tuple<Query, IDictionary<TKey, T>> tup;
                if ((tup = dictOuter.CachedQueries.FirstOrDefault(cq => cq.Item1 == query)) != null)
                {
                    if (tup.Item2 != null)
                    {
                        TypeDict<TKey, T>.Ref.Modify((ref TypeDict<TKey, T> d) => 
                            d.CachedQueries = RemoveOnce(d.CachedQueries, tup).ToArray());
                        return queryFunc(MergeWithLocals(tup.Item2,
                            dictOuter.Entities.Values.Where(query.Check)));
                    }
                    return queryFunc(dictOuter.Entities);
                }

                // we'll need to load entities...
                Shield.SideEffect(null, () => {
                    var qRes = Repository.RunQuery<T>(query);
                    if (qRes == null)
                        throw new ApplicationException(string.Format("Unable to load {0} query {1}", typeof(T).Name, query));
                    ImportTransaction(() => TypeDict<TKey, T>.Ref.Modify((ref TypeDict<TKey, T> d) => {
                        if (qRes.QueryOwned && d.CachedQueries.Any(cq => cq.Item1 == query))
                            return;
                        ImportInt(qRes.Owned, d.Entities);
                        d.CachedQueries = qRes.QueryOwned ?
                            d.CachedQueries.Concat(new[] { Tuple.Create(query, (IDictionary<TKey, T>)null) }).ToArray() :
                            d.CachedQueries.Concat(new[] { Tuple.Create(query,
                                MergeAndFilter(qRes.Result, d.Entities)) }).ToArray();
                    }));
                });
                Shield.Rollback();
                return default(TRes);
            });
        }

        static IDictionary<TKey, T> MergeAndFilter<TKey, T>(IEnumerable<T> source, ShieldedDict<TKey, T> entities) where T : DistributedBase<TKey>
        {
            var res = new Dictionary<TKey, T>();
            foreach (var entity in source)
            {
                T oldMerged;
                if (res.TryGetValue(entity.Id, out oldMerged))
                {
                    Merge(entity, oldMerged);
                    continue;
                }

                T oldTracked;
                if (entities.TryGetValue(entity.Id, out oldTracked))
                {
                    Merge(entity, oldTracked);
                    res.Add(entity.Id, oldTracked);
                }
                else
                    res.Add(entity.Id, Map.ToShielded(entity));
            }
            return res;
        }

        static IDictionary<TKey, T> MergeWithLocals<TKey, T>(IDictionary<TKey, T> res, IEnumerable<T> locals) where T : DistributedBase<TKey>
        {
            foreach (var item in locals)
                res[item.Id] = item;
            return res;
        }

        static IEnumerable<T> RemoveOnce<T>(IEnumerable<T> source, T item)
        {
            var comparer = EqualityComparer<T>.Default;
            bool removed = false;
            foreach (var x in source)
            {
                if (removed || !comparer.Equals(x, item))
                    yield return x;
                else
                    removed = true;
            }
        }

        public static T Add<TKey, T>(T entity) where T : DistributedBase<TKey>, new()
        {
            var dict = TypeDict<TKey, T>.Ref.Value;
            if (dict.Entities.ContainsKey(entity.Id))
                throw new InvalidOperationException("Entity of the same type with same ID already known.");
            var res = Map.ToShielded(entity);
            if (dict.CachedQueries.Any(cq => cq.Item2 == null && cq.Item1.Check(res)))
                dict.Entities.Add(entity.Id, res);
            return res;
        }

        public static DistributedBase Update(DistributedBase entity)
        {
            return (DistributedBase)_updates.Get(entity.GetType()).Invoke(null, new object[] { entity });
        }

        private static Genericize _updates = new Genericize(t => typeof(EntityDictionary)
            .GetMethods(BindingFlags.Public | BindingFlags.Static)
            .FirstOrDefault(mi => mi.IsGenericMethod && mi.Name == "Update")
            .MakeGenericMethod(t.GetProperty("Id").PropertyType, t));

        public static T Update<TKey, T>(T entity) where T : DistributedBase<TKey>, new()
        {
            var dict = TypeDict<TKey, T>.Ref.Value;
            var owned = dict.CachedQueries.Any(cq => cq.Item2 == null && cq.Item1.Check(entity));
            T old;
            if (!dict.Entities.TryGetValue(entity.Id, out old))
            {
                if (owned)
                    throw new InvalidOperationException("Entity does not exist.");
                return Map.ToShielded(entity);
            }
            if (entity.Version < old.Version)
                throw new ConcurrencyException();
            Map.Copy(old.GetType().BaseType, entity, old);
            if (!owned)
                dict.Entities.Remove(entity.Id);
            return old;
        }

        public static void Remove<TKey, T>(T entity) where T : DistributedBase<TKey>
        {
            var dict = TypeDict<TKey, T>.Ref.Value;
            T existing;
            if (!dict.Entities.TryGetValue(entity.Id, out existing) &&
                dict.CachedQueries.Any(cq => cq.Item2 == null && cq.Item1.Check(entity)))
                throw new KeyNotFoundException();
            if (existing != null)
            {
                if (entity.Version < existing.Version)
                    throw new ConcurrencyException();
                dict.Entities.Remove(entity.Id);
            }
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

        static Genericize _imports = new Genericize(t => typeof(EntityDictionary)
            .GetMethod("Import", BindingFlags.NonPublic | BindingFlags.Static)
            .MakeGenericMethod(t.GetProperty("Id").PropertyType, t));

        public static void Import(IEnumerable<DistributedBase> entities)
        {
            ImportTransaction(() => {
                foreach (var typeGrp in entities.GroupBy(e => e.GetType()))
                    _imports.Get(typeGrp.Key).Invoke(null, new object[] { typeGrp });
            });
        }

        static void Import<TKey, T>(IEnumerable<DistributedBase> dtos) where T : DistributedBase<TKey>, new()
        {
            ImportInt(dtos.Cast<T>(), TypeDict<TKey, T>.Ref.Value.Entities);
        }

        static void ImportTransaction(Action act)
        {
            if (Shield.IsInTransaction)
                throw new InvalidOperationException("Import can not be a part of a bigger transaction.");
            try
            {
                _importTransaction = true;
                Shield.InTransaction(act);
            }
            finally
            {
                _importTransaction = false;
            }
        }

        static void ImportInt<TKey, T>(IEnumerable<T> dtos, ShieldedDict<TKey, T> dict) where T : DistributedBase<TKey>, new()
        {
            foreach (var dto in dtos)
            {
                T old;
                if (dict.TryGetValue(dto.Id, out old))
                    Merge(dto, old);
                else
                    dict[dto.Id] = Map.ToShielded(dto);
            }
        }

        static void Merge(DistributedBase dto, DistributedBase old)
        {
            if (dto.Version > old.Version)
                Map.Copy(old.GetType().BaseType, dto, old);
        }

        static Genericize _invalidates = new Genericize(t => typeof(EntityDictionary)
            .GetMethod("Invalidate", BindingFlags.NonPublic | BindingFlags.Static)
            .MakeGenericMethod(t.GetProperty("Id").PropertyType, t));

        public static void Invalidate(IEnumerable<DistributedBase> invalidate)
        {
            Shield.InTransaction(() => {
                foreach (var typeGrp in invalidate.GroupBy(e => e.GetType()))
                    _invalidates.Get(typeGrp.Key).Invoke(null, new object[] { typeGrp });
            });
        }

        static void Invalidate<TKey, T>(IEnumerable<DistributedBase> entities) where T : DistributedBase<TKey>, new()
        {
            TypeDict<TKey, T>.Ref.Modify((ref TypeDict<TKey, T> d) => {
                foreach (var e in entities.Cast<T>())
                {
                    T old;
                    if (d.Entities.TryGetValue(e.Id, out old))
                    {
                        if (d.CachedQueries.Any(q => q.Item2 == null && q.Item1.Check(old)))
                            d.CachedQueries = d.CachedQueries
                                .Where(q => q.Item2 != null || !q.Item1.Check(old)).ToArray();
                        d.Entities.Remove(e.Id);
                    }
                }
            });
        }

        static Genericize _performs = new Genericize(t => typeof(EntityDictionary)
            .GetMethod("PerformExtern", BindingFlags.NonPublic | BindingFlags.Static)
            .MakeGenericMethod(t.GetProperty("Id").PropertyType, t));

        public static bool PerformExtern(IEnumerable<DataOp> ops)
        {
            foreach (var typeGrp in ops.GroupBy(op => op.Entity.GetType()))
            {
                var perform = _performs.Get(typeGrp.Key);
                foreach (var op in typeGrp)
                    if (!(bool)perform.Invoke(null, new object[] { op.OpType, op.Entity }))
                        return false;
            }
            return true;
        }

        static bool PerformExtern<TKey, T>(DataOpType opType, T dto) where T : DistributedBase<TKey>, new()
        {
            var dict = TypeDict<TKey, T>.Ref.Value;
            T existing;
            if (!dict.Entities.TryGetValue(dto.Id, out existing))
            {
                if (!dict.CachedQueries.Any(cq => cq.Item2 == null && cq.Item1.Check(dto)))
                    return true; // not interested
                
                if (opType != DataOpType.Insert)
                    return false;
                dict.Entities[dto.Id] = Map.ToShielded(dto);
                return true;
            }
            if (opType == DataOpType.Insert || dto.Version <= existing.Version)
                return false;
            if (opType == DataOpType.Delete)
                dict.Entities.Remove(dto.Id);
            else
                Merge(dto, existing);
            return true;
        }
    }
}

