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

namespace ShieldedDb.Data
{
    public class Context : IDisposable
    {
        NpgsqlConnection _conn;

        internal IDbConnection Connection
        {
            get { return _conn; }
        }

        internal Context(string connectionString)
        {
            _conn = new NpgsqlConnection(connectionString);
        }

        static ShieldedDict<int, Test> _tests;
        static object _testsLock = new object();

        public IDictionary<int, Test> Tests
        {
            get
            {
                if (_tests == null)
                {
                    lock (_testsLock)
                    {
                        if (_tests == null)
                        {
                            _tests = new ShieldedDict<int, Test>(
                                Connection.Query<Test>("select * from test")
                                    .Select(t => {
                                        var shT = Factory.NewShielded<Test>();
                                        shT.Id = t.Id;
                                        shT.Val = t.Val;
                                        shT.Saved = true;
                                        return new KeyValuePair<int, Test>(t.Id, shT);
                                    }));
                        }
                    }
                }
                return _tests;
            }
        }

        public void Dispose()
        {
            if (_conn == null)
                return;

            _conn.Dispose();
            _conn = null;
        }
    }
}

