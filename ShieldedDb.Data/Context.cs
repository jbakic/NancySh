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
        static readonly object _testsLock = new object();

        public IDictionary<int, Test> Tests
        {
            get
            {
                if (_tests == null)
                    Database.DictionaryFault(_testsLock, ref _tests);
                return _tests;
            }
        }
    }
}

