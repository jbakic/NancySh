using System;
using System.Linq;
using Nancy.Hosting.Self;
using Nancy;
using Shielded.Distro;
using System.Configuration;
using Npgsql;
using nancySh.Models;

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

            DTModule.InitBackend(config, id);
            StaticConfiguration.DisableErrorTraces = false;
            using (var host = new NancyHost(new Uri(server.BaseUrl), new Application()))
            {
                host.Start();
                Console.WriteLine("Hello World @ {0} -- Press Enter to quit...", server.BaseUrl);
                Console.ReadLine();
            }
        }
    }
}
