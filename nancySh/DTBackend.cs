using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using ShieldedDb.Data;
using System.Runtime.Serialization.Json;

namespace nancySh
{
    public class DTransaction
    {
        public Guid Id;
        public List<DataOp> Operations;
    }

    public class DataList
    {
        public List<DistributedBase> Entities;
    }

    public class DTBackend : TwoPCBackend
    {
        private ServerConfig _config;
        private int _myId;

        public DTBackend(ServerConfig config, int myId)
        {
            _config = config;
            _myId = myId;
        }

        static Task<bool> GetResponseAsync(WebRequest req)
        {
            return req.GetResponseAsync()
                .ContinueWith(taskResp =>
                    taskResp.Exception == null &&
                    ((HttpWebResponse)taskResp.Result).StatusCode == HttpStatusCode.OK);
        }

        static Task<bool> WhenAllSucceed(Guid transactionId, IEnumerable<Task<bool>> tasks)
        {
            return Task.WhenAll(tasks)
                .ContinueWith(boolsTask => {
                    if (boolsTask.Exception != null)
                        return false;
                    var res = boolsTask.Result.All(b => b);
                    Console.WriteLine("{0} {1}", res ? "Success" : "Fail", transactionId);
                    return res;
                });
        }

        protected override Task<bool> Prepare(Guid transactionId, IEnumerable<DataOp> ops)
        {
            Console.WriteLine("Preparing transaction {0}", transactionId);
            var trans = new DTransaction { Id = transactionId, Operations = ops.ToList() };
            return WhenAllSucceed(transactionId,
                _config.Servers.Where(s => s.Id != _myId).Select(s => {
                    var serializer = new DataContractJsonSerializer(typeof(DTransaction));
                    var req = WebRequest.Create(s.BaseUrl + "/dt/prepare");
                    req.Method = "POST";
                    req.ContentType = "application/json";
                    serializer.WriteObject(req.GetRequestStream(), trans);
                    return GetResponseAsync(req);
                }));
        }

        protected override Task<bool> Commit(Guid transactionId)
        {
            Console.WriteLine("Committing transaction {0}", transactionId);
            return WhenAllSucceed(transactionId,
                _config.Servers.Where(s => s.Id != _myId).Select(s => {
                    var req = WebRequest.Create(
                        string.Format("{0}/{1}/{2}", s.BaseUrl, "dt/commit", transactionId));
                    req.Method = "POST";
                    req.ContentLength = 0;
                    return GetResponseAsync(req);
                }));
        }

        protected override void Abort(Guid transactionId)
        {
            Console.WriteLine("Aborting transaction {0}", transactionId);
            foreach (var server in _config.Servers.Where(s => s.Id != _myId))
            {
                var req = WebRequest.Create(string.Format(
                    "{0}/{1}/{2}", server.BaseUrl, "dt/abort", transactionId));
                req.Method = "POST";
                req.ContentLength = 0;
                req.BeginGetResponse(ar => { }, null);
            }
        }

        public override T[] LoadAll<T>()
        {
            Console.WriteLine("Loading all {0}", typeof(T).Name);
            var server = SelectRandomServer();
            var req = WebRequest.Create(string.Format(
                "{0}/{1}/{2}", server.BaseUrl, "dt/list", typeof(T).Name));
            var resp = (HttpWebResponse)req.GetResponse();
            Console.WriteLine("Load complete with HTTP {0}", resp.StatusCode);
            if (resp.StatusCode != HttpStatusCode.OK)
                return new T[0];
            var serializer = new DataContractJsonSerializer(typeof(DataList));
            return ((DataList)serializer.ReadObject(resp.GetResponseStream())).Entities.Cast<T>().ToArray();
        }

        Server SelectRandomServer()
        {
            var ind = new Random().Next(_config.Servers.Length);
            if (_config.Servers[ind].Id == _myId)
                ind = (ind + 1) % _config.Servers.Length;
            return _config.Servers[ind];
        }

        public bool PrepareExt(DTransaction trans)
        {
            Console.WriteLine("Preparing external {0}", trans.Id);
            var res = MsgPrepare(trans.Id, trans.Operations);
            Console.WriteLine("{0} {1}", res ? "Success" : "Fail", trans.Id);
            return res;
        }

        public bool CommitExt(Guid transId)
        {
            Console.WriteLine("Committing external {0}", transId);
            var res = MsgCommit(transId);
            Console.WriteLine("{0} {1}", res ? "Success" : "Fail", transId);
            return res;
        }

        public void AbortExt(Guid transId)
        {
            Console.WriteLine("Aborting external {0}", transId);
            MsgAbort(transId);
            Console.WriteLine("Done {0}", transId);
        }
    }
}

