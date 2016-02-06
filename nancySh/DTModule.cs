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

namespace nancySh
{
    public class DTModule : NancyModule
    {
        static DTBackend _backend;

        public static void InitBackend(ServerConfig config, int myId)
        {
            _backend = new DTBackend(config, myId);
            Repository.AddBackend(_backend);
            Test.Repo.GetAll();
        }

        public DTModule() : base("dt")
        {
            Get["/list/Test"] = _ => {
                var serializer = new DataContractJsonSerializer(typeof(DataList));
                var dataList = new DataList {
                    Entities = Repository.InTransaction(() => Repository.GetAll<int, Test>()
                        .Select(Map.NonShieldedClone)
                        .Cast<DistributedBase>()
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

