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
        static DataDeamon _deamon;

        static readonly ConcurrentDictionary<object, Action> _refActions =
            new ConcurrentDictionary<object, Action>();
        static readonly ConcurrentDictionary<Type, Action<object>> _typeActions =
            new ConcurrentDictionary<Type, Action<object>>();

        static Database()
        {
            var entityType = typeof(IEntity);
            Factory.PrepareTypes(Assembly.GetAssembly(entityType)
                .GetTypes().Where(t => t.IsClass && t.GetInterface(entityType.Name) != null).ToArray());

            Shield.WhenCommitting(tf => {
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
            });
        }

        /// <summary>
        /// A dictionary is needed, but missing. The dict is loaded on another thread, while this thread
        /// waits. After waiting, current transaction is rolled back, to be able to read the new dictionary.
        /// </summary>
        internal static void DictionaryFault<TKey, T>(Shielded<ShieldedDict<TKey, T>> dict) where T : class, IEntity<TKey>, new()
        {
            if (dict.Value != null)
                throw new InvalidOperationException();
            _deamon.LoadDict<T, TKey>(dict);
            Shield.Rollback();
        }

        internal static void RegisterDictionary<TKey, T>(ShieldedDict<TKey, T> dict) where T : IEntity<TKey>, new()
        {
            if (!_typeActions.TryAdd(typeof(T), o => {
                    var entity = (T)o;
                    if (entity.Saved && dict.ContainsKey(entity.Id))
                        _deamon.Update(entity);
                }))
            {
                throw new InvalidOperationException("This type already has a registered dictionary.");
            }
            _refActions[dict] = () => Process(dict);
        }

        static void Process<TKey, T>(ShieldedDict<TKey, T> dict) where T : IEntity<TKey>, new()
        {
            foreach (var key in dict.Changes)
            {
                T entity;
                if (!dict.TryGetValue(key, out entity))
                    _deamon.Delete<T, TKey>(key);
                else if (!entity.Saved)
                    _deamon.Insert(entity);
            }
        }

        public static void StartDeamon(string connectionString)
        {
            _deamon = new DataDeamon(connectionString);
        }

        public static void StopDeamon()
        {
            _deamon.Dispose();
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

