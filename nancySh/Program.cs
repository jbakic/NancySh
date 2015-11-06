using System;
using Nancy.Hosting.Self;
using Nancy;
using ShieldedDb.Data;
using System.Configuration;

namespace nancySh
{
    class MainClass
    {
        public static void Main(string[] args)
        {
            Console.WriteLine("Hello World @ port 8080 / Press Enter to quit...");

            StaticConfiguration.DisableErrorTraces = false;
            try
            {
                Database.OpenConnection(ConfigurationManager.AppSettings["DatabaseConnectionString"]);
                using (var host = new NancyHost(new Uri("http://localhost:8080/"), new Application()))
                {
                    host.Start();
                    Console.ReadLine();
                }
            }
            finally
            {
                Database.CloseConnection();
            }
        }
    }
}
