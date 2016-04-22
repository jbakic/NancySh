using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using Shielded.Distro;
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

    public class OwnershipQuery : Query
    {
        public int ServerId;
        public int ServerCount;

        public static bool Owns(DistributedBase d, int server, int serverCount)
        {
            var hash = d.IdValue.GetHashCode();
            var test1 = hash % serverCount + 1;
            var test2 = hash > serverCount ? (hash / serverCount) % serverCount + 1 : (test1 + 1) % serverCount + 1;
            return server == test1 || server == test2;
        }

        public override bool Check(DistributedBase d)
        {
            return Owns(d, ServerId, ServerCount);
        }

        public override bool Equals(Query other)
        {
            var ownOther = other as OwnershipQuery;
            return ownOther != null &&
                ownOther.ServerId == ServerId &&
                ownOther.ServerCount == ServerCount;
        }

        public override int GetHashCode()
        {
            unchecked
            {
                var hash = 17;
                hash = hash * 23 + base.GetHashCode();
                hash = hash * 23 + ServerId.GetHashCode();
                hash = hash * 23 + ServerCount.GetHashCode();
                return hash;
            }
        }

        public override string ToString()
        {
            return string.Format("OwnershipQuery({0}/{1})", ServerId, ServerCount);
        }
    }

    public class DTBackend : TwoPCBackend
    {
        private readonly ServerConfig _config;
        private readonly int _myId;

        public readonly Query Ownership;

        public DTBackend(ServerConfig config, Server myServer)
            : base(myServer.BackupDbConnString == null ? null :
                new SqlBackend(() => new Npgsql.NpgsqlConnection(myServer.BackupDbConnString)))
        {
            _config = config;
            _myId = myServer.Id;
            Ownership = myServer.BackupDbConnString != null ? (Query)Query.All : new OwnershipQuery
            {
                ServerId = _myId,
                ServerCount = _config.Servers.Length,
            };
        }

        IEnumerable<Server> Owners(IEnumerable<DataOp> ops)
        {
            return _config.Servers.Where(s => s.Id != _myId &&
                (s.BackupDbConnString != null ||
                    ops.Any(op => OwnershipQuery.Owns(op.Entity, s.Id, _config.Servers.Length))));
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

        static Task<BackendResult> WhenAllMerge(Guid transactionId, IEnumerable<Task<BackendResult>> tasks)
        {
            return Task.WhenAll(tasks)
                .ContinueWith(backsTask => {
                    if (backsTask.Exception != null)
                        return new BackendResult(false);
                    var res = backsTask.Result.All(b => b.Ok);
                    Console.WriteLine("{0} {1}", res ? "Success" : "Fail", transactionId);
                    return BackendResult.Merge(backsTask.Result);
                });
        }

        protected override Task<BackendResult> Prepare(Guid transactionId, IEnumerable<DataOp> ops)
        {
            Console.WriteLine("Preparing transaction {0}", transactionId);
            var trans = new DTransaction { Id = transactionId, Operations = ops.ToList() };
            return WhenAllMerge(transactionId,
                Owners(ops).Select(s => {
                    try
                    {
                        var serializer = new DataContractJsonSerializer(typeof(DTransaction));
                        var req = WebRequest.Create(s.BaseUrl + "/dt/prepare");
                        req.Method = "POST";
                        req.ContentType = "application/json";
                        serializer.WriteObject(req.GetRequestStream(), trans);
                        return req.GetResponseAsync().ContinueWith(taskResp =>
                            taskResp.Exception != null ? new BackendResult(false) :
                            ((HttpWebResponse)taskResp.Result).StatusCode != HttpStatusCode.OK ? new BackendResult(ops) :
                            new BackendResult(true));
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine("Prepare {0} - {1}", transactionId, ex.Message);
                        return Task.FromResult(new BackendResult(false));
                    }
                }));
        }

        protected override Task Commit(Guid transactionId, IEnumerable<DataOp> ops)
        {
            Console.WriteLine("Committing transaction {0}", transactionId);
            return WhenAllSucceed(transactionId,
                Owners(ops).Select(s => {
                    try
                    {
                        var req = WebRequest.Create(
                            string.Format("{0}/{1}/{2}", s.BaseUrl, "dt/commit", transactionId));
                        req.Method = "POST";
                        req.ContentLength = 0;
                        return GetResponseAsync(req);
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine("Commit {0} - {1}", transactionId, ex.Message);
                        return Task.FromResult(false);
                    }
                })).ContinueWith(t => {
                    if (!t.Result)
                        throw new ApplicationException("Failed commit!");
                });
        }

        protected override void Abort(Guid transactionId, IEnumerable<DataOp> ops)
        {
            Console.WriteLine("Aborting transaction {0}", transactionId);
            foreach (var server in Owners(ops))
            {
                try
                {
                    var req = WebRequest.Create(string.Format(
                        "{0}/{1}/{2}", server.BaseUrl, "dt/abort", transactionId));
                    req.Method = "POST";
                    req.ContentLength = 0;
                    req.BeginGetResponse(ar => { }, null);
                }
                catch (Exception ex)
                {
                    Console.WriteLine("Abort {0} - {1}", transactionId, ex.Message);
                }
            }
        }

        protected override QueryResult<T> DoQuery<T>(Query query)
        {
            var name = typeof(T).Name;
            Console.WriteLine("Running query {0}/{1}", name, query);
            bool owned = query == Ownership;
            var results = _config.Servers.Where(s => s.Id != _myId)
                .AsParallel()
                .WithDegreeOfParallelism(_config.Servers.Length > 1 ? _config.Servers.Length - 1 : 1)
                .WithExecutionMode(ParallelExecutionMode.ForceParallelism)
                .Select(s => {
                    try
                    {
                        var querySerializer = new DataContractJsonSerializer(typeof(Query));
                        var req = WebRequest.Create(string.Format("{0}/{1}/{2}", s.BaseUrl, "dt/query", name));
                        req.Method = "POST";
                        req.ContentType = "application/json";
                        querySerializer.WriteObject(req.GetRequestStream(), query);

                        var resp = (HttpWebResponse)req.GetResponse();
                        if (resp.StatusCode != HttpStatusCode.OK)
                            return Tuple.Create(s, new QueryResult<T>(false));
                        var listSerializer = new DataContractJsonSerializer(typeof(DataList));
                        var l = (DataList)listSerializer.ReadObject(resp.GetResponseStream());
                        return Tuple.Create(s, new QueryResult<T>(owned, l.Entities != null ? l.Entities.Cast<T>() : null));
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine("Query - {0}", ex.Message);
                        return Tuple.Create(s, new QueryResult<T>(false));
                    }
                }).ToArray();
            Console.WriteLine("Loaded {0}", results.Sum(r => r.Item2.Result.Count()));
            if (owned && IsResultComplete(results))
                return new QueryResult<T>(true, results.SelectMany(r => r.Item2.Result));
            return new QueryResult<T>(false, results.SelectMany(r => r.Item2.Result));
        }

        private bool IsResultComplete<T>(Tuple<Server, QueryResult<T>>[] results) where T : DistributedBase
        {
            return
                Backup != null ||
                results.Any(r => r.Item1.BackupDbConnString != null && r.Item2.QueryOwned) ||
                // to allow them to work without any DB configured.
                results.All(r => r.Item2.QueryOwned);
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

