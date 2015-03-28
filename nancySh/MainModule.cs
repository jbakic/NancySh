using System;
using System.Linq;
using Nancy;
using Nancy.Security;
using ShieldedDb.Data;
using ShieldedDb.Models;

namespace nancySh
{
    public class HelloModule : NancyModule
    {
        public HelloModule()
        {
            Get["/"] = parameters => IndexView();

            Post["/delete/{Id:int}"] = parameters => {
                this.ValidateCsrfToken();
                Database.Execute(ctx => { ctx.Tests.Remove(parameters.Id); });
                return IndexView();
            };

            Post["/new"] = _ => Database.Execute(ctx => {
                this.ValidateCsrfToken();
                var t = ctx.Tests.New(new Random().Next(1000));
                t.Val = "Test " + t.Id;
                return IndexView();
            });
        }

        private object IndexView()
        {
            this.CreateNewCsrfToken();
            return View["index", Database.Execute(ctx => ctx.Tests.Values.OrderBy(t => t.Id).ToArray())];
        }
    }
}