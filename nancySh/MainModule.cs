using System;
using System.Linq;
using Nancy;
using Nancy.Security;
using Shielded.Distro;
using nancySh.Models;
using Nancy.ModelBinding;
using System.Runtime.Serialization.Json;
using System.Collections.Generic;

namespace nancySh
{
    public class HelloModule : NancyModule
    {
        public HelloModule()
        {
            Get["/"] = _ => View["index.html"];

            Get["/list"] = _ => Response.AsJson(
                Repository.InTransaction(() => Test.Repo.GetAll()
                    .Select(Map.NonShieldedClone)
                    .OrderBy(t => t.Id)
                    .ToArray()));

            Get["/id/{Id:int}"] = parameters => Repository.InTransaction(() => {
                int id = parameters.Id;
                return Response.AsJson(Map.NonShieldedClone(Test.Repo.Find(id)));
            });

            Post["/delete/{Id:int}/{Version:int}"] = parameters => {
//                this.ValidateCsrfToken();
                Test.Repo.Remove(new Test {
                    Id = parameters.Id,
                    Version = parameters.Version,
                });
                return HttpStatusCode.OK;
            };

            Post["/update"] = parameters => {
//                this.ValidateCsrfToken();
                Test.Repo.Update(this.Bind<Test>());
                return HttpStatusCode.OK;
            };

            Post["/new"] = _ => {
//                this.ValidateCsrfToken();
                var id = new Random().Next(1000);
                Test.Repo.Insert(new Test {
                    Id = id,
                    Val = "Test " + id,
                });
                return HttpStatusCode.OK;
            };
        }
    }
}