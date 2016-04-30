using System;
using System.Collections.Generic;
using System.Linq;
using Nancy;
using Shielded.Distro;
using nancySh.Models;
using Nancy.ModelBinding;

namespace nancySh
{
    public class IdAndName
    {
        public IdAndName() { }

        public IdAndName(Account a)
        {
            Id = a.Id;
            Name = a.Owner;
        }

        public int Id;
        public string Name;
    }

    public class Detail
    {
        public Account Account;
        public List<Booking> Bookings;
    }

    public class TransferReq
    {
        public int SourceId;
        public int TargetId;
        public decimal Change;
    }

    public class AccountModule : NancyModule
    {
        private T LogException<T>(string loc, Func<T> f)
        {
            try
            {
                return f();
            }
            catch (Exception ex)
            {
                Console.WriteLine(loc + ": " + ex);
                throw;
            }
        }

        public AccountModule() : base("account")
        {
            Get["all"] = _ => LogException("get all", () => Response.AsJson(
                Repository.InTransaction(() =>
                    Account.Repo.GetAll()
                    .Select(p => new IdAndName(p))
                    .OrderBy(p => p.Name)
                    .ToArray())));

            Post["new"] = p => LogException("new acc", () => Response.AsJson(Repository.InTransaction(() => {
                var data = this.Bind<IdAndName>();
                return new IdAndName(Account.New(data.Name, 1000m));
            })));

            Get["{id:int}"] = p => LogException("get one", () => Repository.InTransaction(() => {
                int id = p.Id;
                var acc = Account.Repo.TryFind(id);
                if (acc == null)
                    return HttpStatusCode.NotFound;
                return Response.AsJson(new Detail {
                    Account = Map.NonShieldedClone(acc),
                    Bookings = acc.Bookings.Select(Map.NonShieldedClone)
                        .OrderByDescending(b => b.UtcTime).Take(20).ToList(),
                });
            }));

            Post["transfer"] = p => LogException("transfer", () => Repository.InTransaction(() => {
                var data = this.Bind<TransferReq>();
                var time = DateTime.UtcNow;
                Account.Repo.Find(data.SourceId).Book(time, -data.Change);
                Account.Repo.Find(data.TargetId).Book(time, data.Change);
                return HttpStatusCode.OK;
            }));
        }
    }
}
