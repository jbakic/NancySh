using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
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
    /// Provides access to distributed objects and methods for performing
    /// transactional operations on them.
    /// </summary>
    public static class Repository
    {
        class TransactionMeta
        {
            public readonly Dictionary<DistributedBase, DataOp> ToDo = new Dictionary<DistributedBase, DataOp>();
        }

        /// <summary>
        /// Default timeout of all transactions, local and external.
        /// </summary>
        public static int TransactionTimeout = 5000;

        static object _knownTypesLock = new object();

        /// <summary>
        /// Gets all detected distributed entity types.
        /// </summary>
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

        /// <summary>
        /// Detects distributed entity types, and runs some preparations for them. This is
        /// important to be able to serialize entities generically.
        /// </summary>
        /// <param name="assemblies">Optional enumerable of assemblies to look through.</param>
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

        /// <summary>
        /// Register a new backend.
        /// </summary>
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

        /// <summary>
        /// Run a distributed transaction. The lambda gets repeated in case of local conflicts only.
        /// In case of failure to commit in the backends, throws a <see cref="ConcurrencyException"/>.
        /// </summary>
        public static void InTransaction(Action act)
        {
            if (_ctx != null)
            {
                act();
                return;
            }

            try
            {
                using (var continuation = Shield.RunToCommit(TransactionTimeout, () => {
                    _ctx = new TransactionMeta();
                    act();
                }))
                {
                    if (continuation.Completed)
                    {
                        if (continuation.Committed)
                            return;
                        throw new ConcurrencyException();
                    }

                    if (continuation.Fields.Where(tf => tf.HasChanges).Select(tf => tf.Field)
                        .OfType<DistributedBase>().Any(d => !_ctx.ToDo.ContainsKey(d)))
                    {
                        throw new InvalidOperationException("All updates must be declared.");
                    }

                    var distro = RunDistro(continuation);
                    if (!distro.Wait(TransactionTimeout))
                        throw new ConcurrencyException();
                    if (!distro.Result.Ok)
                    {
                        continuation.TryRollback();
                        if (distro.Result.Update != null)
                            EntityDictionary.Import(distro.Result.Update);
                        if (distro.Result.Invalidate != null)
                            EntityDictionary.Invalidate(distro.Result.Invalidate);
                        throw new ConcurrencyException();
                    }
                }
            }
            finally
            {
                _ctx = null;
            }
        }

        /// <summary>
        /// Run a distributed transaction and return its result. The lambda gets repeated in case
        /// of local conflicts only.
        /// In case of failure to commit in the backends, throws a <see cref="ConcurrencyException"/>.
        /// </summary>
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
            var idQuery = query as QueryByIds<TKey>;
            if (idQuery != null)
            {
                return idQuery.Ids.Select(id => {
                    T res;
                    if (dict.TryGetValue(id, out res))
                        return res;
                    return null;
                }).Where(e => e != null);
            }
            return dict.Values.Where(query.Check);
        }

        /// <summary>
        /// Get all entities of type T which satisfy the query. Executes over all backends, returning
        /// everything known and visible at query time.
        /// </summary>
        public static IEnumerable<T> GetAll<TKey, T>(Query query) where T : DistributedBase<TKey>, new()
        {
            return EntityDictionary.Query<TKey, T, IEnumerable<T>>(dict => QueryCheck(dict, query), query);
        }

        /// <summary>
        /// Own the specified query. All backends will be queried for the results, and at least one
        /// of them must succeed in producing an Owned result. On failure, retries a certain number
        /// of times, with pauses in between, and finally crashes the process if unsuccessful.
        /// (Subject to future change :))
        /// This mechanism is crude - it is crucial that other servers know you own something, otherwise
        /// your server will not be included in all transactions it should be. That will cause
        /// differing reads from different servers, and more needless conflicts.
        /// </summary>
        public static void Own<TKey, T>(Query query) where T : DistributedBase<TKey>, new()
        {
            Shield.SideEffect(() =>
                EntityDictionary.ReloadTask<TKey, T>(query));
        }

        /// <summary>
        /// Returns true if the query is owned.
        /// </summary>
        public static bool Owns<TKey, T>(Query query) where T : DistributedBase<TKey>, new()
        {
            return EntityDictionary.OwnsQuery<TKey, T>(query);
        }

        /// <summary>
        /// Get all entities of type T that we own and have locally, and that satisfy the query.
        /// No backends get queried for this.
        /// </summary>
        public static IEnumerable<T> GetLocal<TKey, T>(Query query) where T : DistributedBase<TKey>, new()
        {
            return EntityDictionary.Query<TKey, T, IEnumerable<T>>(dict => QueryCheck(dict, query), null);
        }

        /// <summary>
        /// Find the entity of type T with the given id, or throw otherwise.
        /// </summary>
        public static T Find<TKey, T>(TKey id) where T : DistributedBase<TKey>, new()
        {
            return
                EntityDictionary.Query<TKey, T, T>(dict => dict.ContainsKey(id) ? dict[id] : null, null) ??
                EntityDictionary.Query<TKey, T, T>(dict => dict[id], new QueryByIds<TKey>(id));
        }

        /// <summary>
        /// Find the entity of type T with the given id, or return null.
        /// </summary>
        public static T TryFind<TKey, T>(TKey id) where T : DistributedBase<TKey>, new()
        {
            return
                EntityDictionary.Query<TKey, T, T>(dict => dict.ContainsKey(id) ? dict[id] : null, null) ??
                EntityDictionary.Query<TKey, T, T>(dict => dict.ContainsKey(id) ? dict[id] : null, new QueryByIds<TKey>(id));
        }

        static DataOpType? Already(DistributedBase entity)
        {
            DataOp exist;
            return _ctx.ToDo.TryGetValue(entity, out exist) ? (DataOpType?)exist.OpType : null;
        }

        /// <summary>
        /// Remove the entity.
        /// </summary>
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
        /// You may make further changes on the returned object.
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
        /// Returns the "live", distributed entity. All updates must be declared with this
        /// method. If you just change properties on a live entity (obtained e.g. from Find)
        /// you will get an exception at commit.
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
