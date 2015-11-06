using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Linq;
using System.Reflection;
using Dapper;
using Shielded;
using Shielded.ProxyGen;
using Npgsql;
using ShieldedDb.Models;
using System.Threading;

namespace ShieldedDb.Data
{
    public static class Database
    {
        static readonly ConcurrentDictionary<Type, Action<object>> _saveActions =
            new ConcurrentDictionary<Type, Action<object>>();

        static Sql _sql;

        static Database()
        {
            var entityType = typeof(IEntity);
            Factory.PrepareTypes(Assembly.GetAssembly(entityType)
                .GetTypes().Where(t => t.IsClass && t.GetInterface(entityType.Name) != null).ToArray());

            Shield.WhenCommitting(OnCommitting);
        }

        static void OnCommitting(IEnumerable<TransactionField> tf)
        {
            if (_ctx == null)
                return;
            foreach (var f in tf)
            {
                if (!f.HasChanges) continue;

                var fieldType = f.Field.GetType();
                var isDict = fieldType.IsGenericType &&
                    fieldType.GetGenericTypeDefinition() == typeof(ShieldedDict<,>);
                var type = isDict ? fieldType.GetGenericArguments()[1] : fieldType.BaseType;

                Action<object> actObj;
                if (_saveActions.TryGetValue(type, out actObj))
                    actObj(isDict ? null : f.Field);
            }
        }

        /// <summary>
        /// A dictionary is needed, but missing. Causes the current transaction to rollback,
        /// and loads the dictionary before the new repetition starts.
        /// </summary>
        internal static void DictionaryFault<TKey, T>(Shielded<ShieldedDict<TKey, T>> dict) where T : class, IEntity<TKey>, new()
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
            _saveActions.TryAdd(typeof(T), o => {
                if (o == null)
                {
                    foreach (var key in dict.Changes)
                    {
                        T entity;
                        if (!dict.TryGetValue(key, out entity))
                            _sql.Delete<T, TKey>(key);
                        else if (!entity.Saved)
                            _sql.Insert(entity);
                    }
                }
                else
                {
                    var entity = (T)o;
                    if (!dict.Changes.Contains(entity.Id) && entity.Saved && dict.ContainsKey(entity.Id))
                        _sql.Update(entity);
                }
            });
        }

        static void Process<TKey, T>(ShieldedDict<TKey, T> dict) where T : IEntity<TKey>, new()
        {
        }

        public static void SetConnectionString(string connectionString)
        {
            _sql = new Sql(connectionString);
        }

        [ThreadStatic]
        static Context _ctx;

        public static void Execute(Action<Context> act)
        {
            if (_ctx != null)
            {
                act(_ctx);
                return;
            }

            try
            {
                _ctx = new Context();
                Shield.InTransaction(() => act(_ctx));
            }
            finally
            {
                _ctx = null;
            }
        }

        public static T Execute<T>(Func<Context, T> f)
        {
            T res = default(T);
            Execute(ctx => {
                res = f(ctx);
            });
            return res;
        }
    }
}

