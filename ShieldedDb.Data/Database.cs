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
        static readonly ConcurrentDictionary<object, Action> _refActions =
            new ConcurrentDictionary<object, Action>();
        static readonly ConcurrentDictionary<Type, Action<object>> _typeActions =
            new ConcurrentDictionary<Type, Action<object>>();

        static Connection _conn;

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

                Action act;
                if (_refActions.TryGetValue(f.Field, out act))
                {
                    act();
                    continue;
                }
                Action<object> actObj;
                if (_typeActions.TryGetValue(f.Field.GetType().BaseType, out actObj))
                    actObj(f.Field);
            }
        }

        /// <summary>
        /// A dictionary is needed, but missing.
        /// </summary>
        internal static void DictionaryFault<TKey, T>(Shielded<ShieldedDict<TKey, T>> dict) where T : class, IEntity<TKey>, new()
        {
            Shield.SideEffect(null, () => {
                bool win = false;
                using (var cont = Shield.RunToCommit(10000, () => {
                    win = false;
                    if (dict.Value != null)
                        return;
                    win = true;
                    dict.Value = null; // force write, we need to lock it
                }))
                {
                    if (!win)
                        return;
                    var d = _conn.LoadDict<T, TKey>();
                    cont.InContext(_ => dict.Value = d);
                    cont.Commit();
                }
                RegisterDictionary(dict.Value);
            });
            Shield.Rollback();
        }

        static void RegisterDictionary<TKey, T>(ShieldedDict<TKey, T> dict) where T : IEntity<TKey>, new()
        {
            if (!_typeActions.TryAdd(typeof(T), o => 
                {
                    var entity = (T)o;
                    if (!dict.Changes.Contains(entity.Id) &&
                        entity.Saved && dict.ContainsKey(entity.Id))
                        _conn.Update(entity);
                }))
            {
                return;
            }
            _refActions[dict] = () => Process(dict);
        }

        static void Process<TKey, T>(ShieldedDict<TKey, T> dict) where T : IEntity<TKey>, new()
        {
            foreach (var key in dict.Changes)
            {
                T entity;
                if (!dict.TryGetValue(key, out entity))
                    _conn.Delete<T, TKey>(key);
                else if (!entity.Saved)
                    _conn.Insert(entity);
            }
        }

        public static void OpenConnection(string connectionString)
        {
            _conn = new Connection(connectionString);
        }

        public static void CloseConnection()
        {
            _conn.Dispose();
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

