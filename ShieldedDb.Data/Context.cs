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

        public IDictionary<int, Test> Tests
        {
            get
            {
                if (_tests.Value == null)
                    Database.DictionaryFault(_tests);
                return _tests.Value;
            }
        }
        static readonly Shielded<ShieldedDict<int, Test>> _tests = new Shielded<ShieldedDict<int, Test>>();
    }
}

