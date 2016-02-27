using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Reflection;
using Shielded;
using Shielded.ProxyGen;
using System.Diagnostics;
using System.Threading.Tasks;
using System.Threading;

namespace Shielded.Distro
{
    /// <summary>
    /// It's annoying that one type parameter is not enough, and we always
    /// must type in the key and then the entity type, even though the latter
    /// completely identifies both. So, this class. If it could be static and
    /// be base class for other classes, but so that they specify these type
    /// args, that would be great. But no.
    /// </summary>
    public class Accessor<TKey, T> where T : DistributedBase<TKey>, new()
    {
        public IEnumerable<T> GetAll() { return Repository.GetAll<TKey, T>(Query.All); }

        public T Find(TKey id) { return Repository.Find<TKey, T>(id); }

        public void Remove(T entity) { Repository.Remove<TKey, T>(entity); }

        /// <summary>
        /// Returns the "live", distributed entity, which will be ref-equal to your
        /// object if it is a proxy already, or a new shielded proxy otherwise.
        /// </summary>
        public T Insert(T entity) { return Repository.Insert<TKey, T>(entity); }

        /// <summary>
        /// Returns the "live", distributed entity.
        /// </summary>
        public T Update(T entity) { return Repository.Update<TKey, T>(entity); }
    }

    public static class Repository
    {
        class TransactionMeta
        {
            public readonly Dictionary<DistributedBase, DataOp> ToDo = new Dictionary<DistributedBase, DataOp>();
        }

        public static int TransactionTimeout = 5000;

        static object _knownTypesLock = new object();
        public static IEnumerable<Type> KnownTypes
        {
            get;
            private set;
        }

        static Repository()
        {
            DetectEntityTypes();
            Shield.WhenCommitting<DistributedBase>(ds => {
                if (_ctx == null && !_externTransaction && !EntityDictionary.IsImporting)
                    throw new InvalidOperationException("Distributables can only be changed in repo transactions.");
            });
        }

        public static void DetectEntityTypes(IEnumerable<Assembly> assemblies = null)
        {
            var iDist = typeof(DistributedBase);
            var distBase = typeof(DistributedBase<>);
            Type[] types;
            lock (_knownTypesLock)
            {
                types = (KnownTypes ?? Enumerable.Empty<Type>())
                    .Concat((assemblies ?? AppDomain.CurrentDomain.GetAssemblies())
                        .SelectMany(asm => asm.GetTypes().Where(t =>
                            t.IsClass && t != distBase && t.IsSubclassOf(iDist) && !Factory.IsProxy(t))))
                    .Distinct()
                    .ToArray();
                KnownTypes = types;
            }
            Debug.WriteLine("Preparing {0} types.", types.Length);
            Factory.PrepareTypes(types);
        }

        static void DetectUpdates(CommitContinuation cont)
        {
            cont.InContext(tfs => {
                foreach (var field in tfs)
                {
                    if (!field.HasChanges)
                        continue;
                    var entity = field.Field as DistributedBase;
                    if (entity == null || _ctx.ToDo.ContainsKey(entity))
                        continue;
                    entity = EntityDictionary.Update(entity);
                    _ctx.ToDo.Add(entity, DataOp.Update(entity));
                }
            });
        }

        static DataOp NonShClone(DataOp source)
        {
            return new DataOp {
                OpType = source.OpType,
                Entity = Map.NonShieldedClone(source.Entity),
            };
        }

        static Task<BackendResult> RunDistro(CommitContinuation cont)
        {
            DataOp[] todos = null;
            cont.InContext(() => {
                todos = _ctx.ToDo.Values
                    .Where(op => op.OpType != DataOpType.Ignore)
                    .Select(op => {
                        op.Entity.Version = op.Entity.Version + 1;
                        return NonShClone(op);
                    })
                    .ToArray();
            });
            if (!todos.Any())
                return Task.FromResult(new BackendResult(true));
            return Task.WhenAll(
                _backs.Select(b => b.Run(todos)))
                    .ContinueWith(res => BackendResult.Merge(res.Result));
        }

        static IBackend[] _backs = new IBackend[0];

        public static void AddBackend(IBackend back)
        {
            IBackend[] oldBacks, newBacks = _backs;
            do
            {
                oldBacks = newBacks;
                newBacks = oldBacks.Concat(new[] { back }).ToArray();
            } while ((newBacks = Interlocked.CompareExchange(ref _backs, newBacks, oldBacks)) != oldBacks);
        }


        [ThreadStatic]
        static TransactionMeta _ctx;

        public static bool InTransaction(Action act)
        {
            if (_ctx != null)
            {
                act();
                return true;
            }

            try
            {
                using (var continuation = Shield.RunToCommit(TransactionTimeout, () => {
                    _ctx = new TransactionMeta();
                    act();
                }))
                {
                    if (continuation.Completed)
                        return continuation.Committed;
                    DetectUpdates(continuation);
                    var distro = RunDistro(continuation);
                    if (!distro.Wait(TransactionTimeout))
                        return false;
                    if (!distro.Result.Ok)
                    {
                        continuation.TryRollback();
                        if (distro.Result.Update != null)
                            EntityDictionary.Import(distro.Result.Update);
                        if (distro.Result.Invalidate != null)
                            EntityDictionary.Invalidate(distro.Result.Invalidate);
                        return false;
                    }
                    return continuation.TryCommit();
                }
            }
            finally
            {
                _ctx = null;
            }
        }

        public static T InTransaction<T>(Func<T> f)
        {
            T res = default(T);
            InTransaction(() => { res = f(); });
            return res;
        }

        internal static QueryResult<T> RunQuery<T>(Query query) where T : DistributedBase, new()
        {
            return _backs.QueryParallelSafe(b => b.Query<T>(query));
        }

        static IEnumerable<T> QueryCheck<TKey, T>(IDictionary<TKey, T> dict, Query query) where T : DistributedBase<TKey>, new()
        {
            var idQuery = query as QueryById<TKey>;
            if (idQuery != null)
            {
                T res;
                if (dict.TryGetValue(idQuery.Id, out res))
                    return new[] { res };
                return Enumerable.Empty<T>();
            }
            return dict.Values.Where(query.Check);
        }

        public static IEnumerable<T> GetAll<TKey, T>(Query query) where T : DistributedBase<TKey>, new()
        {
            return EntityDictionary.Query<TKey, T, IEnumerable<T>>(dict => QueryCheck(dict, query), query);
        }

        public static void Own<TKey, T>(Query query) where T : DistributedBase<TKey>, new()
        {
            Shield.SideEffect(() =>
                EntityDictionary.ReloadTask<TKey, T>(query));
        }

        public static bool Owns<TKey, T>(Query query) where T : DistributedBase<TKey>, new()
        {
            return EntityDictionary.OwnsQuery<TKey, T>(query);
        }

        public static IEnumerable<T> GetLocal<TKey, T>(Query query) where T : DistributedBase<TKey>, new()
        {
            return EntityDictionary.Query<TKey, T, IEnumerable<T>>(dict => QueryCheck(dict, query), null);
        }

        public static T Find<TKey, T>(TKey id) where T : DistributedBase<TKey>, new()
        {
            return
                EntityDictionary.Query<TKey, T, T>(dict => dict.ContainsKey(id) ? dict[id] : null, null) ??
                EntityDictionary.Query<TKey, T, T>(dict => dict[id], new QueryById<TKey>(id));
        }

        static DataOpType? Already(DistributedBase entity)
        {
            DataOp exist;
            return _ctx.ToDo.TryGetValue(entity, out exist) ? (DataOpType?)exist.OpType : null;
        }

        public static void Remove<TKey, T>(T entity) where T : DistributedBase<TKey>, new()
        {
            InTransaction(() => {
                entity = EntityDictionary.Remove<TKey, T>(entity);
                if (Already(entity) == DataOpType.Insert)
                    _ctx.ToDo[entity] = DataOp.Ignore(entity);
                else
                    _ctx.ToDo[entity] = DataOp.Delete(entity);
            });
        }

        /// <summary>
        /// Returns the "live", distributed entity, which will be ref-equal to your
        /// object if it is a proxy already, or a new shielded proxy otherwise.
        /// </summary>
        public static T Insert<TKey, T>(T entity) where T : DistributedBase<TKey>, new()
        {
            return InTransaction(() => {
                entity = EntityDictionary.Add<TKey, T>(entity);
                if (Already(entity) == DataOpType.Delete)
                    _ctx.ToDo[entity].OpType = DataOpType.Update;
                else
                    _ctx.ToDo[entity] = DataOp.Insert(entity);
                return entity;
            });
        }

        /// <summary>
        /// Returns the "live", distributed entity.
        /// </summary>
        public static T Update<TKey, T>(T entity) where T : DistributedBase<TKey>, new()
        {
            return InTransaction(() => {
                entity = EntityDictionary.Update<TKey, T>(entity);
                var already = Already(entity);
                if (already == DataOpType.Delete)
                    throw new KeyNotFoundException("Entity with given key does not exist.");
                if (!already.HasValue)
                    _ctx.ToDo[entity] = DataOp.Update(entity);
                return entity;
            });
        }

        [ThreadStatic]
        static bool _externTransaction;

        // prevents our WhenCommitting check from breaking the commit due to this not being a regular repo transaction.
        private class NoCheckContinuation : CommitContinuation
        {
            private readonly CommitContinuation _cont;

            public NoCheckContinuation(CommitContinuation inner)
            {
                _cont = inner;
            }

            public override bool TryCommit()
            {
                try
                {
                    _externTransaction = true;
                    return _cont.TryCommit();
                }
                finally
                {
                    _externTransaction = false;
                }
            }

            public override bool TryRollback() { return _cont.TryRollback(); }
            public override void InContext(Action act) { _cont.InContext(act); }
            public override TransactionField[] Fields { get { return _cont.Fields; } }
        }

        /// <summary>
        /// If it's OK with the ops, it will return a continuation, otherwise null.
        /// </summary>
        public static CommitContinuation PrepareExtern(IEnumerable<DataOp> ops, int? timeoutOverride = null)
        {
            if (Shield.IsInTransaction)
                throw new InvalidOperationException("Not allowed in transaction.");
            bool outcome = false;
            var cont = Shield.RunToCommit(timeoutOverride ?? TransactionTimeout, () => {
                outcome = EntityDictionary.PerformExtern(ops);
            });
            if (outcome)
                return new NoCheckContinuation(cont);
            cont.TryRollback();
            return null;
        }
    }
}
