using System;
using Nancy.Hosting.Self;
using Nancy;
using ShieldedDb.Data;

namespace nancySh
{
    class MainClass
    {
        public static void Main(string[] args)
        {
            Console.WriteLine("Hello World @ 127.0.0.1:8080 / Press Enter to quit...");

            StaticConfiguration.DisableErrorTraces = false;
            try
            {
                using (var host = new NancyHost(new Uri("http://127.0.0.1:8080/"), new Application()))
                {
                    Database.Execute(ctx => { });
                    host.Start();
                    Console.ReadLine();
                }
            }
            finally
            {
                Database.StopDeamon();
            }
        }
    }
}
