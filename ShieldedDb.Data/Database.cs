using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;

using Dapper;
using Shielded;
using Npgsql;

using ShieldedDb.Models;

namespace ShieldedDb.Data
{
    public static class Database
    {
        static DataDeamon _deamon;

        static Database()
        {
            _deamon = new DataDeamon(ConfigurationManager.AppSettings["DatabaseConnectionString"]);
            Shield.WhenCommitting(tf => {
                if (_ctx == null || tf.All(field => field.Field != _ctx.Tests))
                    return;

                foreach (var key in ((ShieldedDict<int, Test>)_ctx.Tests).Changes)
                {
                    if (!_ctx.Tests.ContainsKey(key))
                        _deamon.Delete<Test, int>(new Test { Id = key });
                    else
                        _deamon.Insert(_ctx.Tests[key]);
                }
            });
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
                _ctx = new Context(ConfigurationManager.AppSettings["DatabaseConnectionString"]);
                Shield.InTransaction(() => act(_ctx));
            }
            finally
            {
                _ctx.Dispose();
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

