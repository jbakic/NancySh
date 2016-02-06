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

        protected override Task<BackendResult> Prepare(Guid transactionId, IEnumerable<DataOp> ops)
        {
            Console.WriteLine("Preparing transaction {0}", transactionId);
            var trans = new DTransaction { Id = transactionId, Operations = ops.ToList() };
            return WhenAllSucceed(transactionId,
                _config.Servers.Where(s => s.Id != _myId).Select(s => {
                    try
                    {
                        var serializer = new DataContractJsonSerializer(typeof(DTransaction));
                        var req = WebRequest.Create(s.BaseUrl + "/dt/prepare");
                        req.Method = "POST";
                        req.ContentType = "application/json";
                        serializer.WriteObject(req.GetRequestStream(), trans);
                        return GetResponseAsync(req);
                    }
                    catch
                    {
                        return Task.FromResult(false);
                    }
                })).ContinueWith(boolTask => boolTask.Result ?
                    new BackendResult(true) : new BackendResult(ops));
        }

        protected override Task Commit(Guid transactionId)
        {
            Console.WriteLine("Committing transaction {0}", transactionId);
            return WhenAllSucceed(transactionId,
                _config.Servers.Where(s => s.Id != _myId).Select(s => {
                    try
                    {
                        var req = WebRequest.Create(
                            string.Format("{0}/{1}/{2}", s.BaseUrl, "dt/commit", transactionId));
                        req.Method = "POST";
                        req.ContentLength = 0;
                        return GetResponseAsync(req);
                    }
                    catch
                    {
                        return Task.FromResult(false);
                    }
                })).ContinueWith(t => {
                    if (!t.Result)
                        throw new ApplicationException("Failed commit!");
                });
        }

        protected override void Abort(Guid transactionId)
        {
            Console.WriteLine("Aborting transaction {0}", transactionId);
            foreach (var server in _config.Servers.Where(s => s.Id != _myId))
            {
                try
                {
                    var req = WebRequest.Create(string.Format(
                        "{0}/{1}/{2}", server.BaseUrl, "dt/abort", transactionId));
                    req.Method = "POST";
                    req.ContentLength = 0;
                    req.BeginGetResponse(ar => { }, null);
                }
                catch { }
            }
        }

        public override IEnumerable<T> LoadAll<T>()
        {
            var name = typeof(T).Name;
            Console.WriteLine("Loading all {0}", name);
            return _config.Servers.Where(s => s.Id != _myId).SelectManyParallelSafe(s => {
                try
                {
                    var req = WebRequest.Create(string.Format(
                        "{0}/{1}/{2}", s.BaseUrl, "dt/list", name));
                    var resp = (HttpWebResponse)req.GetResponse();
                    Console.WriteLine("Load complete with HTTP {0}", resp.StatusCode);
                    if (resp.StatusCode != HttpStatusCode.OK)
                        return null;
                    var serializer = new DataContractJsonSerializer(typeof(DataList));
                    var l = (DataList)serializer.ReadObject(resp.GetResponseStream());
                    Console.WriteLine("Loaded {0}", l.Entities == null ? "null" : l.Entities.Count.ToString());
                    return l.Entities != null ? l.Entities.Cast<T>() : null;
                }
                catch
                {
                    return null;
                }
            }) ?? Enumerable.Empty<T>(); // if all have empty, empty is all.
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

