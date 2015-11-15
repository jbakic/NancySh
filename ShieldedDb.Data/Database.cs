using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Reflection;
using Shielded;
using Shielded.ProxyGen;

namespace ShieldedDb.Data
{
    public static class Database
    {
        static readonly ConcurrentDictionary<Type, Func<object, SqlOp>> _typeActions =
            new ConcurrentDictionary<Type, Func<object, SqlOp>>();
        static readonly ConcurrentDictionary<object, Func<SqlOp>> _refActions =
            new ConcurrentDictionary<object, Func<SqlOp>>();

        static Sql _sql;

        static Database()
        {
            Shield.WhenCommitting(OnCommitting);
        }

        static void OnCommitting(IEnumerable<TransactionField> tf)
        {
            if (_ctx != null)
                _sql.Run(GetOps(tf));
        }

        static IEnumerable<SqlOp> GetOps(IEnumerable<TransactionField> tf)
        {
            foreach (var f in tf)
            {
                if (!f.HasChanges) continue;

                Func<SqlOp> refAct;
                if (_refActions.TryGetValue(f.Field, out refAct))
                {
                    yield return refAct();
                    continue;
                }

                Func<object, SqlOp> objAct;
                if (_typeActions.TryGetValue(f.Field.GetType().BaseType, out objAct))
                    yield return objAct(f.Field);
            }
        }

        /// <summary>
        /// A dictionary is needed, but missing. Causes the current transaction to rollback,
        /// and loads the dictionary before the new repetition starts.
        /// </summary>
        public static void LoadAll<TKey, T>(Shielded<ShieldedDict<TKey, T>> dict) where T : class, IEntity<TKey>, new()
        {
            Shield.SideEffect(null, () => {
                // we run a dummy trans just to lock the wrapping Shielded...
                using (var cont = Shield.RunToCommit(10000, () => dict.Value = dict.Value))
                {
                    // ...then check if it's still empty, in which case we do the loading
                    // conveniently, although it's locked, we can still read the dict wrapper out of transaction..
                    if (dict.Value != null)
                        return;
                    // loading runs a transaction while loading, so it may not run in-context.
                    var d = _sql.LoadDict<T, TKey>();
                    cont.InContext(_ => dict.Value = d);
                    RegisterDictionary(d);
                    cont.Commit();
                }
            });
            Shield.Rollback();
        }

        static void RegisterDictionary<TKey, T>(ShieldedDict<TKey, T> dict) where T : IEntity<TKey>, new()
        {
            if (!_typeActions.TryAdd(typeof(T), o =>
                {
                    var entity = (T)o;
                    if (!dict.Changes.Contains(entity.Id) && entity.Inserted && dict.ContainsKey(entity.Id))
                        return _sql.Update(entity);
                    return Sql.Nop;
                }))
            {
                throw new InvalidOperationException("Type already registered.");
            }
            _refActions.TryAdd(dict, () => Sql.Do(
                dict.Changes.Select(key => {
                    T entity;
                    if (!dict.TryGetValue(key, out entity))
                        return _sql.Delete<T, TKey>(key);
                    else if (!entity.Inserted)
                        return _sql.Insert(entity);
                    return Sql.Nop;
                })));
        }


        public static Func<IDbConnection> ConnectionFactory
        {
            set
            {
                _sql = new Sql(value);
            }
        }

        [ThreadStatic]
        static IContext _ctx;

        public static void Execute<Ctx>(Action<Ctx> act) where Ctx : IContext, new()
        {
            if (_ctx != null)
            {
                act((Ctx)_ctx);
                return;
            }

            try
            {
                var ctx = new Ctx();
                _ctx = ctx;
                Shield.InTransaction(() => act(ctx));
            }
            finally
            {
                _ctx = null;
            }
        }

        public static T Execute<Ctx, T>(Func<Ctx, T> f) where Ctx : IContext, new()
        {
            T res = default(T);
            Execute<Ctx>(ctx => {
                res = f(ctx);
            });
            return res;
        }
    }
}

