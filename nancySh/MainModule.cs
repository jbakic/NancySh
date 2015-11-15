using System;
using System.Linq;
using Nancy;
using Nancy.Security;
using ShieldedDb.Data;
using ShieldedDb.Models;
using Nancy.ModelBinding;

namespace nancySh
{
    public class HelloModule : NancyModule
    {
        public HelloModule()
        {
            Get["/"] = parameters => IndexView();

            Post["/delete/{Id:int}"] = parameters => {
                this.ValidateCsrfToken();
                Database.Execute((TestContext ctx) => {
                    ctx.Tests.Remove(parameters.Id);
                });
                return Response.AsRedirect("/");
            };

            Post["/update"] = parameters => {
                this.ValidateCsrfToken();
                var data = this.Bind<Test>();
                Database.Execute((TestContext ctx) => {
                    var shT = ctx.Tests[data.Id];
                    shT.Val = data.Val;
                });
                return Response.AsRedirect("/");
            };

            Post["/new"] = _ => {
                this.ValidateCsrfToken();
                Database.Execute((TestContext ctx) => {
                    var t = ctx.Tests.New(new Random().Next(1000));
                    t.Val = "Test " + t.Id;
                });
                return Response.AsRedirect("/");
            };
        }

        private object IndexView()
        {
            this.CreateNewCsrfToken();
            return View["index", Database.Execute((TestContext ctx) => ctx.Tests.Values.OrderBy(t => t.Id).ToArray())];
        }
    }
}