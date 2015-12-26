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
                Test.Repo.Remove(new Test { Id = parameters.Id });
                return Response.AsRedirect("/");
            };

            Post["/update"] = parameters => {
                this.ValidateCsrfToken();
                var data = this.Bind<Test>();
                Repository.InTransaction(() =>
                    // the .Val change results in an automatic DB update as well.
                    Test.Repo.Find(data.Id).Val = data.Val);
                return Response.AsRedirect("/");
            };

            Post["/new"] = _ => {
                this.ValidateCsrfToken();
                var id = new Random().Next(1000);
                Test.Repo.Insert(new Test {
                    Id = id,
                    Val = "Test " + id,
                });
                return Response.AsRedirect("/");
            };
        }

        private object IndexView()
        {
            this.CreateNewCsrfToken();
            return View["index", Test.Repo.GetAll().OrderBy(t => t.Id).ToArray()];
        }
    }
}