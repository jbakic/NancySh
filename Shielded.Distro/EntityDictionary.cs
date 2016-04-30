using System;
using System.Collections.Generic;
using System.Linq;
using Shielded;
using Shielded.ProxyGen;
using System.Threading;
using System.Reflection;
using System.Collections.Concurrent;
using System.Threading.Tasks;
using System.Diagnostics;

namespace Shielded.Distro
{
    internal delegate TRes QueryFunc<TKey, T, TRes>(IDictionary<TKey, T> dict) where T : DistributedBase<TKey>, new();

    /// <summary>
    /// Global repo of all live entities.
    /// </summary>
    static class EntityDictionary
    {
        struct TypeDict<TKey, T> where T : DistributedBase<TKey>, new()
        {
            public ShieldedDict<TKey, T> Entities;
            public Query[] OwnedQueries;
            public Tuple<Query, ShieldedDict<TKey, T>>[] CachedQueries;

            public static Shielded<TypeDict<TKey, T>> Ref = new Shielded<TypeDict<TKey, T>>(
                new TypeDict<TKey, T> {
                    Entities = new ShieldedDict<TKey, T>(),
                    OwnedQueries = new Query[0],
                    CachedQueries = new Tuple<Query, ShieldedDict<TKey, T>>[0],
                });

            public static void Import(Query query, QueryResult<T> qRes)
            {
                ImportTransaction(() => Ref.Modify((ref TypeDict<TKey, T> d) => {
                    if (qRes.QueryOwned)
                    {
                        ImportInt(qRes.Result, d.Entities);
                        if (!d.OwnedQueries.Contains(query))
                            d.OwnedQueries = d.OwnedQueries.Concat(new[] { query }).ToArray();
                    }
                    else
                        d.CachedQueries = d.CachedQueries.Concat(new[] {
                            Tuple.Create(query, MergeAndFilter(qRes.Result, d.Entities, query)) }).ToArray();
                }));
            }

            static ShieldedDict<TKey, T> MergeAndFilter(IEnumerable<T> source, ShieldedDict<TKey, T> entities, Query query)
            {
                var res = new ShieldedDict<TKey, T>();
                foreach (var entity in entities.Values.Where(query.Check).Concat(source))
                {
                    T oldMerged;
                    if (res.TryGetValue(entity.Id, out oldMerged))
                        Merge(entity, oldMerged);
                    else
                        res.Add(entity.Id, Map.ToShielded(entity));
                }
                return res;
            }

            public static ShieldedDict<TKey, T> PickUp(Query query)
            {
                var oldDict = Ref.Value;
                if (oldDict.OwnedQueries.Contains(query))
                    return oldDict.Entities;
                if (oldDict.CachedQueries.All(cq => cq.Item1 != query))
                    return null;
                ShieldedDict<TKey, T> res = null;
                Ref.Modify((ref TypeDict<TKey, T> d) => {
                    var tup = d.CachedQueries.First(cq => cq.Item1 == query);
                    d.CachedQueries = RemoveOnce(d.CachedQueries, tup).ToArray();
                    res = tup.Item2;
                });
                return res;
            }

            public static void UpdateCached(T entity)
            {
                var dict = Ref.Value;
                var mapped = Map.ToShielded(entity);
                foreach (var cache in dict.CachedQueries)
                {
                    if (cache.Item1.Check(entity))
                        cache.Item2[entity.Id] = mapped;
                    else
                        cache.Item2.Remove(entity.Id);
                }
            }

            public static void RemoveCached(T entity)
            {
                var dict = Ref.Value;
                foreach (var cache in dict.CachedQueries)
                    cache.Item2.Remove(entity.Id);
            }
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

        public static bool OwnsQuery<TKey, T>(Query query) where T : DistributedBase<TKey>, new()
        {
            return TypeDict<TKey, T>.Ref.Value.OwnedQueries.Any(q => q == query);
        }

        public static TRes Query<TKey, T, TRes>(QueryFunc<TKey, T, TRes> queryFunc, Query query) where T : DistributedBase<TKey>, new()
        {
            return Shield.InTransaction(() => {
                if (query == null)
                    return queryFunc(TypeDict<TKey, T>.Ref.Value.Entities);
                var cached = TypeDict<TKey, T>.PickUp(query);
                if (cached != null)
                    return queryFunc(cached);

                // we'll need to load entities...
                Shield.SideEffect(null, () => {
                    var qRes = Repository.RunQuery<T>(query);
                    if (qRes == null)
                        throw new ApplicationException(string.Format("Unable to load {0} query {1}", typeof(T).Name, query));
                    TypeDict<TKey, T>.Import(query, qRes);
                });
                Shield.Rollback();
                return default(TRes);
            });
        }

        private static Type Normalize(Type entityType)
        {
            if (Factory.IsProxy(entityType))
                return entityType.BaseType;
            return entityType;
        }

        public static T Add<TKey, T>(T entity) where T : DistributedBase<TKey>, new()
        {
            var dict = TypeDict<TKey, T>.Ref.Value;
            if (dict.Entities.ContainsKey(entity.Id))
                throw new InvalidOperationException("Entity of the same type with same ID already known.");
            var res = Map.ToShielded(entity);
            if (dict.OwnedQueries.Any(q => q.Check(res)))
                dict.Entities.Add(entity.Id, res);
            TypeDict<TKey, T>.UpdateCached(res);
            return res;
        }

        public static DistributedBase Update(DistributedBase entity)
        {
            return (DistributedBase)_updates.Get(Normalize(entity.GetType())).Invoke(null, new object[] { entity });
        }

        private static Genericize _updates = new Genericize(t => typeof(EntityDictionary)
            .GetMethods(BindingFlags.Public | BindingFlags.Static)
            .FirstOrDefault(mi => mi.IsGenericMethod && mi.Name == "Update")
            .MakeGenericMethod(t.GetProperty("Id").PropertyType, t));

        public static T Update<TKey, T>(T entity) where T : DistributedBase<TKey>, new()
        {
            var dict = TypeDict<TKey, T>.Ref.Value;
            T old;
            if (!dict.Entities.TryGetValue(entity.Id, out old))
            {
                if (dict.OwnedQueries.Any(q => q.Check(entity)))
                    throw new InvalidOperationException("Entity does not exist.");
                var res = Map.ToShielded(entity);
                TypeDict<TKey, T>.UpdateCached(res);
                return res;
            }
            if (entity.Version < old.Version)
                throw new ConcurrencyException();
            Map.Copy(old.GetType().BaseType, entity, old);
            if (!dict.OwnedQueries.Any(q => q.Check(old)))
                dict.Entities.Remove(old.Id);
            TypeDict<TKey, T>.UpdateCached(old);
            return old;
        }

        public static T Remove<TKey, T>(T entity) where T : DistributedBase<TKey>, new()
        {
            var dict = TypeDict<TKey, T>.Ref.Value;
            T existing;
            if (!dict.Entities.TryGetValue(entity.Id, out existing) &&
                dict.OwnedQueries.Any(q => q.Check(entity)))
                throw new KeyNotFoundException();
            if (existing != null)
            {
                if (entity.Version < existing.Version)
                    throw new ConcurrencyException();
                dict.Entities.Remove(entity.Id);
                return existing;
            }
            TypeDict<TKey, T>.RemoveCached(entity);
            return entity;
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
                    dict[dto.Id] = old = Map.ToShielded(dto);
                TypeDict<TKey, T>.UpdateCached(old);
            }
        }

        static void Merge(DistributedBase dto, DistributedBase old)
        {
            if (dto != old && dto.Version > old.Version)
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
                    d.Entities.Remove(e.Id);
                    if (d.OwnedQueries.Any(q => q.Check(e)))
                    {
                        var dropouts = d.OwnedQueries.Where(q => q.Check(e)).ToArray();
                        d.OwnedQueries = d.OwnedQueries.Where(q => !q.Check(e)).ToArray();
                        Shield.SideEffect(() => {
                            foreach (var lost in dropouts)
                                ReloadTask<TKey, T>(lost);
                        });
                    }
                    if (d.CachedQueries.Any(cq => cq.Item1.Check(e)))
                        d.CachedQueries = d.CachedQueries.Where(cq => !cq.Item1.Check(e)).ToArray();
                }
            });
        }

        public static void ReloadTask<TKey, T>(Query lost) where T : DistributedBase<TKey>, new()
        {
            Task.Run(() => {
                int tries = 0;
                var rnd = new Random();
                while (true)
                {
                    try
                    {
                        Query<TKey, T, bool>((QueryFunc<TKey, T, bool>)(d => true), lost);
                        if (OwnsQuery<TKey, T>(lost))
                            return;
                    }
                    catch { }
                    if (++tries >= 15)
                        // little drastic maybe...
                        Process.GetCurrentProcess().Kill();
                    Thread.Sleep(rnd.Next(500, 1000));
                }
            });
        }

        static Genericize _performs = new Genericize(t => typeof(EntityDictionary)
            .GetMethod("PerformExtern", BindingFlags.NonPublic | BindingFlags.Static)
            .MakeGenericMethod(t.GetProperty("Id").PropertyType, t));

        public static bool PerformExtern(IEnumerable<DataOp> ops)
        {
            foreach (var op in ops)
            {
                var perform = _performs.Get(op.Entity.GetType());
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
                if (!dict.OwnedQueries.Any(q => q.Check(dto)))
                {
                    TypeDict<TKey, T>.UpdateCached(dto);
                    return true; // not interested
                }
                
                if (opType != DataOpType.Insert)
                    return false;
                var entity = dict.Entities[dto.Id] = Map.ToShielded(dto);
                TypeDict<TKey, T>.UpdateCached(entity);
                return true;
            }
            if (opType == DataOpType.Insert || dto.Version <= existing.Version)
                return false;
            if (opType == DataOpType.Delete)
            {
                dict.Entities.Remove(dto.Id);
                TypeDict<TKey, T>.RemoveCached(dto);
            }
            else
            {
                Merge(dto, existing);
                TypeDict<TKey, T>.UpdateCached(existing);
            }
            return true;
        }
    }
}

