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
        public IEnumerable<T> GetAll() { return Repository.GetAll<T>(); }

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
            public readonly Dictionary<IDistributed, DataOp> ToDo = new Dictionary<IDistributed, DataOp>();
        }

        static Repository()
        {
            var iDist = typeof(IDistributed);
            var distBase = typeof(DistributedBase<>);
            var types = AppDomain.CurrentDomain.GetAssemblies()
                .SelectMany(asm =>
                    asm.GetTypes().Where(t =>
                        t.IsClass && t != distBase && t.GetInterface(iDist.Name) != null))
                .ToArray();
            Debug.WriteLine("Preparing {0} types.", types.Length);
            Factory.PrepareTypes(types);

            Shield.WhenCommitting<IDistributed>(ds => {
                if (_ctx == null && !EntityDictionary.IsImporting && ds.Any(EntityDictionary.IsTracked))
                    throw new InvalidOperationException("Tracked entities can only be changed in repo transactions.");
            });
        }

        static void DetectUpdates(IEnumerable<TransactionField> tfs)
        {
            foreach (var field in tfs)
            {
                if (!field.HasChanges)
                    continue;
                var entity = field.Field as IDistributed;
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
            cont.InContext(() => todos = _ctx.ToDo.Values.Select(NonShClone).ToArray());
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
                _ctx = new TransactionMeta();
                using (var continuation = Shield.RunToCommit(30000, act))
                {
                    DetectUpdates(continuation.Fields);
                    return RunDistro(continuation).Result && continuation.TryCommit();
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

        static IEnumerable<T> Loader<T>() where T : IDistributed, new()
        {
            return _backs[0].LoadAll<T>();
        }

        public static IEnumerable<T> GetAll<T>() where T : IDistributed, new()
        {
            return EntityDictionary.Query(dict => dict.Values.Cast<T>(), Loader<T>);
        }

        public static T Find<TKey, T>(TKey id) where T : DistributedBase<TKey>, new()
        {
            return EntityDictionary.Query(dict => (T)dict[id], Loader<T>);
        }

        static bool Already(IDistributed entity, DataOpType opType)
        {
            DataOp exist;
            return _ctx.ToDo.TryGetValue(entity, out exist) && exist.OpType == opType;
        }

        public static void Remove<TKey, T>(T entity) where T : DistributedBase<TKey>
        {
            InTransaction(() => {
                EntityDictionary.Remove(entity);
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
                entity = EntityDictionary.Add(entity);
                if (Already(entity, DataOpType.Delete))
                    _ctx.ToDo[entity].OpType = DataOpType.Update;
                else
                    _ctx.ToDo[entity] = DataOp.Insert(entity);
                return entity;
            });
        }
    }
}
