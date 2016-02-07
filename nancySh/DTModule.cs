using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using Nancy;
using Nancy.ModelBinding;
using ShieldedDb.Data;
using ShieldedDb.Models;
using System.Threading.Tasks;
using System.Runtime.Serialization.Json;
using System.IO;
using System.Reflection;

namespace nancySh
{
    public class DTModule : NancyModule
    {
        static DTBackend _backend;
        static Dictionary<Type, MethodInfo> _getters = new Dictionary<Type, MethodInfo>();

        public static void InitBackend(ServerConfig config, int myId)
        {
            _backend = new DTBackend(config, myId);
            Repository.AddBackend(_backend);
            foreach (var typ in Repository.KnownTypes)
            {
                var method = typeof(Repository)
                    .GetMethod("GetAll", BindingFlags.Public | BindingFlags.Static)
                    .MakeGenericMethod(typ.GetProperty("Id").PropertyType, typ);
                _getters[typ] = method;
                method.Invoke(null, new object[] { false });
            }
        }

        public DTModule() : base("dt")
        {
            Get["/list/{typ}"] = parameters => {
                var serializer = new DataContractJsonSerializer(typeof(DataList));
                var dataList = new DataList {
                    Entities = Repository.InTransaction(() =>
                        ((IEnumerable<DistributedBase>)
                            _getters[Repository.KnownTypes.First(t => t.Name == parameters.typ)]
                            .Invoke(null, new object[] { false }))
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
                return _backend.PrepareExt(trans) ? Nancy.HttpStatusCode.OK : Nancy.HttpStatusCode.BadRequest;
            };

            Post["/commit/{id:guid}"] = parameters => {
                Guid transId = parameters.id;
                return _backend.CommitExt(transId) ? Nancy.HttpStatusCode.OK : Nancy.HttpStatusCode.BadRequest;
            };

            Post["/abort/{id:guid}"] = parameters => {
                Guid transId = parameters.id;
                _backend.AbortExt(transId);
                return Nancy.HttpStatusCode.OK;
            };
        }
    }
}

