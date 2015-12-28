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

namespace ShieldedDb.Data
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
        public IEnumerable<T> GetAll() { return Repository.GetAll<TKey, T>(); }

        public T Find(TKey id) { return Repository.Find<TKey, T>(id); }

        public void Remove(T entity) { Repository.Remove<TKey, T>(entity); }

        /// <summary>
        /// Returns the "live", distributed entity, which will be ref-equal to your
        /// object if it is a proxy already, or a new shielded proxy otherwise.
        /// </summary>
        public T Insert(T entity) { return Repository.Insert<TKey, T>(entity); }
    }

    public static class Repository
    {
        class TransactionMeta
        {
            public readonly Dictionary<DistributedBase, DataOp> ToDo = new Dictionary<DistributedBase, DataOp>();
        }

        public static int TransactionTimeout = 20000;

        public static IEnumerable<Type> KnownTypes;

        static Repository()
        {
            var iDist = typeof(DistributedBase);
            var distBase = typeof(DistributedBase<>);
            var types = AppDomain.CurrentDomain.GetAssemblies()
                .SelectMany(asm =>
                    asm.GetTypes().Where(t =>
                        t.IsClass && t != distBase && t.IsSubclassOf(iDist)))
                .ToArray();
            Debug.WriteLine("Preparing {0} types.", types.Length);
            Factory.PrepareTypes(types);
            KnownTypes = types;

            Shield.WhenCommitting<DistributedBase>(ds => {
                if (_ctx == null && !_externTransaction && !EntityDictionary.IsImporting)
                    throw new InvalidOperationException("Distributables can only be changed in repo transactions.");
            });
        }

        static void DetectUpdates(IEnumerable<TransactionField> tfs)
        {
            foreach (var field in tfs)
            {
                if (!field.HasChanges)
                    continue;
                var entity = field.Field as DistributedBase;
                if (entity == null || _ctx.ToDo.ContainsKey(entity))
                    continue;
                _ctx.ToDo.Add(entity, DataOp.Update(entity));
            }
        }

        static DataOp NonShClone(DataOp source)
        {
            return new DataOp {
                OpType = source.OpType,
                Entity = Map.NonShieldedClone(source.Entity),
            };
        }

        static Task<bool> RunDistro(CommitContinuation cont)
        {
            DataOp[] todos = null;
            cont.InContext(() => {
                foreach (var entity in _ctx.ToDo.Keys)
                    entity.Version = entity.Version + 1;
                todos = _ctx.ToDo.Values.Select(NonShClone).ToArray();
            });
            if (!todos.Any())
                return Task.FromResult(true);
            return Task.WhenAll(
                _backs.Select(b => b.Run(todos)))
                .ContinueWith(boolsTask => boolsTask.Result.All(b => b));
        }

        static IBackend[] _backs = new IBackend[0];

        /// <summary>
        /// Currently, only the first back-end is used for loading.
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
                    DetectUpdates(continuation.Fields);
                    var distro = RunDistro(continuation);
                    return distro.Wait(TransactionTimeout) &&
                        distro.Result &&
                        continuation.TryCommit();
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

        static IEnumerable<T> Loader<T>() where T : DistributedBase, new()
        {
            return _backs[0].LoadAll<T>();
        }

        public static IEnumerable<T> GetAll<TKey, T>(bool localOnly = false) where T : DistributedBase<TKey>, new()
        {
            return EntityDictionary.Query<TKey, T, IEnumerable<T>>(dict => dict.Values,
                localOnly ? (LoaderFunc<T>)null : Loader<T>);
        }

        public static T Find<TKey, T>(TKey id, bool localOnly = false) where T : DistributedBase<TKey>, new()
        {
            return EntityDictionary.Query<TKey, T, T>(dict => dict[id],
                localOnly ? (LoaderFunc<T>)null : Loader<T>);
        }

        static bool Already(DistributedBase entity, DataOpType opType)
        {
            DataOp exist;
            return _ctx.ToDo.TryGetValue(entity, out exist) && exist.OpType == opType;
        }

        public static void Remove<TKey, T>(T entity) where T : DistributedBase<TKey>
        {
            InTransaction(() => {
                EntityDictionary.Remove<TKey, T>(entity);
                if (Already(entity, DataOpType.Insert))
                    _ctx.ToDo.Remove(entity);
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
                if (Already(entity, DataOpType.Delete))
                    _ctx.ToDo[entity].OpType = DataOpType.Update;
                else
                    _ctx.ToDo[entity] = DataOp.Insert(entity);
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

            public override void Commit()
            {
                if (!TryCommit())
                    throw new InvalidOperationException("Commit failed.");
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

            public override void Rollback() { _cont.Rollback(); }
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
