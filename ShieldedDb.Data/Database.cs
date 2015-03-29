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

namespace ShieldedDb.Data
{
    public static class Database
    {
        static DataDeamon _deamon;
        static string _connectionString;

        internal static IDbConnection NewConnection()
        {
            return new NpgsqlConnection(_connectionString);
        }

        static readonly ConcurrentDictionary<object, Action> _refActions =
            new ConcurrentDictionary<object, Action>();
        static readonly ConcurrentDictionary<Type, Action<object>> _typeActions =
            new ConcurrentDictionary<Type, Action<object>>();

        internal static void RegisterDictionary<TKey, T>(ShieldedDict<TKey, T> dict) where T : IEntity<TKey>, new()
        {
            _refActions.TryAdd(dict, () => Process(dict));
            _typeActions.TryAdd(typeof(T), o => {
                var entity = (T)o;
                if (dict.ContainsKey(entity.Id))
                    _deamon.Update<T, TKey>(entity);
            });
        }

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

        static void Process<TKey, T>(ShieldedDict<TKey, T> dict) where T : IEntity<TKey>, new()
        {
            foreach (var key in dict.Changes)
            {
                if (!dict.ContainsKey(key))
                    _deamon.Delete<T, TKey>(new T { Id = key });
                else
                    _deamon.Insert<T, TKey>(dict[key]);
            }
        }

        public static void StartDeamon(string connectionString)
        {
            _connectionString = connectionString;
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

