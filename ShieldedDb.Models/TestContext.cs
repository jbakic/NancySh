﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Shielded;
using Shielded.ProxyGen;
using System.Configuration;

using ShieldedDb.Data;
using System.Reflection;

namespace ShieldedDb.Models
{
    public class TestContext : IContext
    {
        static TestContext()
        {
            var entityType = typeof(IEntity<>);
            Factory.PrepareTypes(Assembly.GetExecutingAssembly()
                .GetTypes().Where(t => t.IsClass && t.GetInterface(entityType.Name) != null).ToArray());
        }

        public IDictionary<int, Test> Tests
        {
            get
            {
                if (_tests.Value == null)
                    Database.LoadAll(_tests);
                return _tests.Value;
            }
        }
        static readonly Shielded<ShieldedDict<int, Test>> _tests = new Shielded<ShieldedDict<int, Test>>();
    }
}
