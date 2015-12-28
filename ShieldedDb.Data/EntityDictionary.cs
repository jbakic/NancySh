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
    public delegate TRes QueryFunc<TKey, T, TRes>(ShieldedDict<TKey, T> dict) where T : DistributedBase<TKey>, new();
    public delegate IEnumerable<T> LoaderFunc<T>();

    /// <summary>
    /// Global repo of all live entities.
    /// </summary>
    static class EntityDictionary
    {
        private struct TypeDict<TKey, T> where T : DistributedBase<TKey>
        {
            public ShieldedDict<TKey, T> Entities;
            public bool HasAll;

            public static Shielded<TypeDict<TKey, T>> Ref = new Shielded<TypeDict<TKey, T>>(
                new TypeDict<TKey, T> { Entities = new ShieldedDict<TKey, T>() });
        }

        public static TRes Query<TKey, T, TRes>(QueryFunc<TKey, T, TRes> query,
            LoaderFunc<T> allGetter = null,
            int allGetterTimeoutMs = Timeout.Infinite) where T : DistributedBase<TKey>, new()
        {
            return Shield.InTransaction(() => {
                var dict = TypeDict<TKey, T>.Ref.Value;
                if (allGetter == null || dict.HasAll)
                    return query(dict.Entities);

                // we'll need to load entities...
                Shield.SideEffect(null, () => {
                    // we run a dummy trans just to lock the key...
                    using (var cont = Shield.RunToCommit(allGetterTimeoutMs, () =>
                        TypeDict<TKey, T>.Ref.Modify((ref TypeDict<TKey, T> td) => { dict = td; })))
                    {
                        // if someone beat us to it...
                        if (dict.HasAll)
                            return;
                        Import(allGetter(), dict.Entities);
                        cont.InContext(() => TypeDict<TKey, T>.Ref.Modify(
                            (ref TypeDict<TKey, T> d) => d.HasAll = true));
                        cont.Commit();
                    }
                });
                Shield.Rollback();
                return default(TRes);
            });
        }

        public static bool HasAll<TKey, T>() where T : DistributedBase<TKey>
        {
            return TypeDict<TKey, T>.Ref.Value.HasAll;
        }

        public static T Add<TKey, T>(T entity) where T : DistributedBase<TKey>, new()
        {
            var dict = TypeDict<TKey, T>.Ref.Value.Entities;
            if (dict.ContainsKey(entity.Id))
                throw new InvalidOperationException("Entity of the same type with same ID already known.");
            var res = Map.ToShielded(entity);
            dict.Add(entity.Id, res);
            return res;
        }

        public static void Remove<TKey, T>(T entity) where T : DistributedBase<TKey>
        {
            var dict = TypeDict<TKey, T>.Ref.Value;
            T existing;
            if (!dict.Entities.TryGetValue(entity.Id, out existing) && dict.HasAll)
                throw new KeyNotFoundException();
            if (existing != null)
            {
                // version was not yet increased
                if (entity.Version != existing.Version)
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

        public static void Import<TKey, T>(IEnumerable<T> dtos) where T : DistributedBase<TKey>, new()
        {
            Import(dtos, TypeDict<TKey, T>.Ref.Value.Entities);
        }

        static void Import<TKey, T>(IEnumerable<T> dtos, ShieldedDict<TKey, T> dict) where T : DistributedBase<TKey>, new()
        {
            if (Shield.IsInTransaction)
                throw new InvalidOperationException("Import can not be a part of a bigger transaction.");
            try
            {
                _importTransaction = true;
                Shield.InTransaction(() => {
                    foreach (var dto in dtos)
                    {
                        T old;
                        if (dict.TryGetValue(dto.Id, out old))
                            Merge(dto, old);
                        else
                            dict[dto.Id] = Map.ToShielded(dto);
                    }
                });
            }
            finally
            {
                _importTransaction = false;
            }
        }

        static void Merge(DistributedBase dto, DistributedBase old)
        {
            if (dto.Version > old.Version)
                Map.Copy(old.GetType().BaseType, dto, old);
        }

        static ConcurrentDictionary<Type, MethodInfo> _performForType = new ConcurrentDictionary<Type, MethodInfo>();

        static MethodInfo GetPerformForDto(DistributedBase dto)
        {
            return typeof(EntityDictionary)
                .GetMethod("PerformExtern", BindingFlags.NonPublic | BindingFlags.Static)
                .MakeGenericMethod(dto.IdValue.GetType(), dto.GetType());
        }

        public static bool PerformExtern(IEnumerable<DataOp> ops)
        {
            foreach (var typeGrp in ops.GroupBy(op => op.Entity.GetType()))
            {
                var perform = _performForType.GetOrAdd(typeGrp.Key, _ => GetPerformForDto(typeGrp.First().Entity));
                foreach (var op in typeGrp)
                    if (!(bool)perform.Invoke(null, new object[] { op.OpType, op.Entity }))
                        return false;
            }
            return true;
        }

        static bool PerformExtern<TKey, T>(DataOpType opType, T dto) where T : DistributedBase<TKey>, new()
        {
            var dict = TypeDict<TKey, T>.Ref.Value.Entities;
            T existing;
            if (!dict.TryGetValue(dto.Id, out existing))
            {
                if (opType != DataOpType.Insert)
                    return false;
                dict[dto.Id] = Map.ToShielded(dto);
                return true;
            }
            if (opType == DataOpType.Insert || dto.Version != existing.Version + 1)
                return false;
            if (opType == DataOpType.Delete)
                dict.Remove(dto.Id);
            else
                Merge(dto, existing);
            return true;
        }
    }
}

