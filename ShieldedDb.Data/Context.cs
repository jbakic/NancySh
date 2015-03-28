using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading;
using Dapper;
using Npgsql;
using Shielded;
using Shielded.ProxyGen;
using ShieldedDb.Models;
using System.Configuration;

namespace ShieldedDb.Data
{
    public class Context
    {
        internal Context()
        {
        }

        static ShieldedDict<int, Test> _tests;
        static object _testsLock = new object();

        public IDictionary<int, Test> Tests
        {
            get
            {
                if (_tests != null)
                    return _tests;
                lock (_testsLock)
                {
                    if (_tests != null)
                        return _tests;
                    using (var conn = new NpgsqlConnection(ConfigurationManager.AppSettings["DatabaseConnectionString"]))
                        _tests = new ShieldedDict<int, Test>(
                            conn.Query<Test>("select * from test")
                                .Select(t => {
                                    var shT = Factory.NewShielded<Test>();
                                    shT.Id = t.Id;
                                    shT.Val = t.Val;
                                    shT.Saved = true;
                                    return new KeyValuePair<int, Test>(t.Id, shT);
                                }));
                }
                return _tests;
            }
        }
    }
}

