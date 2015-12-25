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
        public void Insert(T entity) { Repository.Insert<TKey, T>(entity); }
    }

    public static class Repository
    {
        class TransactionMeta
        {
            public readonly List<DataOp> ToDo = new List<DataOp>();
        }

        static ShieldedDict<Type, object> _dictCache = new ShieldedDict<Type, object>();

        static ShieldedDict<TKey, T> TryGetDict<TKey, T>()
        {
            object obj;
            if (_dictCache.TryGetValue(typeof(T), out obj))
                return (ShieldedDict<TKey, T>)obj;
            return null;
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

            Shield.WhenCommitting<IDistributed>(_ => {
                if (_ctx == null)
                    throw new InvalidOperationException("Distributables can be changed only in repository transactions.");
            });
        }

        static Sql _sql;

        static void DetectUpdates(CommitContinuation continuation)
        {
            continuation.InContext(tfs =>
                _ctx.ToDo.AddRange(
                    tfs.Where(field =>
                        field.HasChanges && field.Field is IDistributed && _ctx.ToDo.All(op => op.Entity != field.Field))
                    .Select(field => DataOp.Update((IDistributed)field.Field))));
        }

        static Task<bool> RunDistro()
        {
            return _sql.Run(_ctx.ToDo);
        }

        public static Func<IDbConnection> ConnectionFactory
        {
            set
            {
                _sql = new Sql(value);
            }
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
                    DetectUpdates(continuation);
                    return RunDistro().Result && continuation.TryCommit();
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

        public static IEnumerable<T> GetAll<TKey, T>() where T : DistributedBase<TKey>, new()
        {
            return InTransaction(() => TryGetDictOrLoad<TKey, T>().Values);
        }

        internal static ShieldedDict<TKey, T> TryGetDictOrLoad<TKey, T>() where T : DistributedBase<TKey>, new()
        {
            var type = typeof(T);
            var dict = TryGetDict<TKey, T>();
            if (dict != null)
                return dict;
            
            Shield.SideEffect(null, () => {
                // we run a dummy trans just to lock the key...
                using (var cont = Shield.RunToCommit(10000, () => {
                    if ((dict = TryGetDict<TKey, T>()) == null)
                        _dictCache[type] = null;
                }))
                {
                    // ...if it's not empty, we just return. the above was a read-only trans.
                    if (dict != null)
                        return;
                    // this is kinda ugly! IDistributed's can only be changed in Repo transactions, so
                    // we fake one, because Map needs it.
                    dict = InFakeTransaction(() =>
                        new ShieldedDict<TKey, T>(
                            _sql.LoadAll<T>()
                            .Select(t => new KeyValuePair<TKey, T>(t.Id, Map.ToShielded(t)))));
                    cont.InContext(_ => _dictCache[type] = dict);
                    cont.Commit();
                }
            });
            Shield.Rollback();
            // never happens:
            return null;
        }

        static T InFakeTransaction<T>(Func<T> f)
        {
            var oldMeta = _ctx;
            try
            {
                _ctx = new TransactionMeta();
                return Shield.InTransaction(f);
            }
            finally
            {
                _ctx = oldMeta;
            }
        }

        public static T Find<TKey, T>(TKey id) where T : DistributedBase<TKey>, new()
        {
            return InTransaction(() => TryGetDictOrLoad<TKey, T>()[id]);
        }

        public static void Remove<TKey, T>(T entity) where T : DistributedBase<TKey>
        {
            InTransaction(() => {
                _ctx.ToDo.Add(DataOp.Delete(entity));
                var dict = TryGetDict<TKey, T>();
                if (dict != null)
                    dict.Remove(entity.Id);
            });
        }

        public static void Insert<TKey, T>(T entity) where T : DistributedBase<TKey>, new()
        {
            InTransaction(() => {
                var dict = TryGetDict<TKey, T>();
                if (dict == null)
                    // to make sure it stays null until the insert is done.
                    _dictCache[typeof(T)] = null;
                else
                    dict.Add(entity.Id, Map.ToShielded(entity));
                _ctx.ToDo.Add(DataOp.Insert(entity));
            });
        }
    }
}
