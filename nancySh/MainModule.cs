using System;
using System.Linq;
using Nancy;
using Nancy.Security;
using Shielded.Distro;
using nancySh.Models;
using Nancy.ModelBinding;

namespace nancySh
{
    public class HelloModule : NancyModule
    {
        public HelloModule()
        {
            Get["/"] = parameters => IndexView();

            Get["/id/{Id:int}"] = parameters => ById(parameters.Id);

            Post["/delete/{Id:int}/{Version:int}"] = parameters => {
//                this.ValidateCsrfToken();
                Test.Repo.Remove(new Test {
                    Id = parameters.Id,
                    Version = parameters.Version,
                });
                return Response.AsRedirect("/");
            };

            Post["/update"] = parameters => {
//                this.ValidateCsrfToken();
                Test.Repo.Update(this.Bind<Test>());
                return Response.AsRedirect("/");
            };

            Post["/new"] = _ => {
//                this.ValidateCsrfToken();
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
            return View["index", Repository.InTransaction(() =>
                Test.Repo.GetAll().Select(Map.NonShieldedClone).OrderBy(t => t.Id).ToArray())];
        }

        private object ById(int id)
        {
            this.CreateNewCsrfToken();
            return View["index", new[] { Repository.InTransaction(() =>
                Map.NonShieldedClone(Test.Repo.Find(id))) }];
        }
    }
}