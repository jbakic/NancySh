using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using Nancy;
using Nancy.ModelBinding;
using Shielded.Distro;
using nancySh.Models;
using System.Threading.Tasks;
using System.Runtime.Serialization.Json;
using System.IO;
using System.Reflection;

namespace nancySh
{
    public class DTModule : NancyModule
    {
        internal static DTBackend Backend;

        public static void InitBackend(ServerConfig config, int myId)
        {
            Backend = new DTBackend(config, config.Servers.Single(s => s.Id == myId));
            Repository.AddBackend(Backend);

            _loads = Backend.Backup == null
                ? new Genericize(t => typeof(Repository)
                    .GetMethod("GetLocal", BindingFlags.Public | BindingFlags.Static)
                    .MakeGenericMethod(t.GetProperty("Id").PropertyType, t))
                : new Genericize(t => typeof(DTModule)
                    .GetMethod("DoubleLoad", BindingFlags.NonPublic | BindingFlags.Static)
                    .MakeGenericMethod(t.GetProperty("Id").PropertyType, t));
            
            foreach (var typ in Repository.KnownTypes)
            {
                var ownMethod = typeof(Repository)
                    .GetMethod("Own", BindingFlags.Public | BindingFlags.Static)
                    .MakeGenericMethod(typ.GetProperty("Id").PropertyType, typ);
                ownMethod.Invoke(null, new object[] { Backend.Ownership });
            }
        }

        static Genericize _loads;

        static IEnumerable<T> DoubleLoad<TKey, T>(Query query) where T : DistributedBase<TKey>, new()
        {
            if (Repository.Owns<TKey, T>(query))
                return Repository.GetLocal<TKey, T>(query);
            return MergeEntities<TKey, T>(
                Repository.GetLocal<TKey, T>(query).Concat(Backend.Backup.Query<T>(query).Result));
        }

        static IEnumerable<T> MergeEntities<TKey, T>(IEnumerable<T> source) where T : DistributedBase<TKey>
        {
            return source.GroupBy(e => e.Id)
                .Select(grp => grp.Aggregate((a, b) => a.Version >= b.Version ? a : b));
        }

        public DTModule() : base("dt")
        {
            Post["/query/{typ}"] = parameters => {
                var typ = Repository.KnownTypes.First(t => t.Name == parameters.typ);
                var querySerializer = new DataContractJsonSerializer(typeof(Query));
                var query = querySerializer.ReadObject(Request.Body);

                var serializer = new DataContractJsonSerializer(typeof(DataList));
                var dataList = new DataList {
                    Entities = Repository.InTransaction(() =>
                        ((IEnumerable<DistributedBase>)_loads.Get(typ).Invoke(null, new[] { query }))
                        .Select(Map.NonShieldedClone)
                        .ToList()),
                };
                return new Response
                {
                    Contents = stream => serializer.WriteObject(stream, dataList),
                    ContentType = "application/json",
                };
            };

            Post["/prepare"] = _ => {
                var serializer = new DataContractJsonSerializer(typeof(DTransaction));
                var trans = (DTransaction)serializer.ReadObject(Request.Body);
                return Backend.PrepareExt(trans) ? Nancy.HttpStatusCode.OK : Nancy.HttpStatusCode.BadRequest;
            };

            Post["/commit/{id:guid}"] = parameters => {
                Guid transId = parameters.id;
                return Backend.CommitExt(transId) ? Nancy.HttpStatusCode.OK : Nancy.HttpStatusCode.BadRequest;
            };

            Post["/abort/{id:guid}"] = parameters => {
                Guid transId = parameters.id;
                Backend.AbortExt(transId);
                return Nancy.HttpStatusCode.OK;
            };
        }
    }
}

