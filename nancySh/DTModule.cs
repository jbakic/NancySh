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
        static Dictionary<Type, MethodInfo> _getters = new Dictionary<Type, MethodInfo>();

        public static void InitBackend(ServerConfig config, int myId)
        {
            Backend = new DTBackend(config, config.Servers.Single(s => s.Id == myId));
            Repository.AddBackend(Backend);
            var ownershipQ = new OwnershipQuery { ServerId = myId, ServerCount = config.Servers.Length };
            foreach (var typ in Repository.KnownTypes)
            {
                var method = typeof(Repository)
                    .GetMethod("GetLocal", BindingFlags.Public | BindingFlags.Static)
                    .MakeGenericMethod(typ.GetProperty("Id").PropertyType, typ);
                _getters[typ] = method;

                var ownedGetter = typeof(Repository)
                    .GetMethod("GetAll", BindingFlags.Public | BindingFlags.Static)
                    .MakeGenericMethod(typ.GetProperty("Id").PropertyType, typ);
                ownedGetter.Invoke(null, new object[] { ownershipQ });
            }
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
                        ((IEnumerable<DistributedBase>)_getters[typ].Invoke(null, new[] { query }))
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

