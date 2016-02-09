﻿using System;
using System.Linq;
using Nancy.Hosting.Self;
using Nancy;
using ShieldedDb.Data;
using System.Configuration;
using Npgsql;
using ShieldedDb.Models;

namespace nancySh
{
    class MainClass
    {
        public static void Main(string[] args)
        {
            if (args.Length < 1)
            {
                Console.WriteLine("Please specify ID of server.");
                return;
            }
            var id = int.Parse(args[0]);
            var config = ServerConfig.Load("ServerConfig.xml");
            var server = config.Servers.First(s => s.Id == id);

            // HACK: needed on MS.NET to load the Models assembly before the Repository goes
            // searching for known types. that whole thing needs to be improved, ofc.
            var hack = new Test();

            Console.WriteLine("Hello World @ {0} -- Press Enter to quit...", server.BaseUrl);
            StaticConfiguration.DisableErrorTraces = false;
            DTModule.InitBackend(config, id);
            using (var host = new NancyHost(new Uri(server.BaseUrl), new Application()))
            {
                host.Start();
                Console.ReadLine();
            }
        }
    }
}
