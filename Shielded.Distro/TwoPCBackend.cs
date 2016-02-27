using System;
using System.Threading.Tasks;
using System.Collections.Generic;
using System.Linq;
using Shielded;

namespace Shielded.Distro
{
    public class TwoPCFailedException : Exception
    {
        public bool FailedDuringCommit { get; set; }

        public TwoPCFailedException() { }

        public TwoPCFailedException(bool failedInCommit, AggregateException inner)
            : base(failedInCommit ? "2PC fail during commit!" : "2PC fail during prepare.", inner)
        {
            FailedDuringCommit = failedInCommit;
        }
    }

    public class TwoPCBackupFailedException : Exception
    {
        public BackendResult Result { get; set; }

        public TwoPCBackupFailedException() { }

        public TwoPCBackupFailedException(BackendResult res)
            : base("Backup of the 2PC backend has failed to commit.")
        {
            Result = res;
        }
    }

    /// <summary>
    /// Base class for a backend which supports 2-phase commit via some kind of
    /// messages.
    /// </summary>
    public abstract class TwoPCBackend : IBackend
    {
        public readonly IBackend Backup;

        protected TwoPCBackend(IBackend backup = null)
        {
            Backup = backup;
        }

        public Task<BackendResult> Run(IEnumerable<DataOp> ops)
        {
            var transId = Guid.NewGuid();
            return Prepare(transId, ops)
                .ContinueWith((Task<BackendResult> prepareTask) => {
                    if (prepareTask.Exception != null)
                    {
                        Abort(transId, ops);
                        throw new TwoPCFailedException(false, prepareTask.Exception);
                    }
                    if (!prepareTask.Result.Ok)
                    {
                        Abort(transId, ops);
                        return prepareTask.Result;
                    }
                    return Commit(transId, ops).ContinueWith(commitTask => {
                        if (commitTask.Exception != null)
                            throw new TwoPCFailedException(true, commitTask.Exception);
                        BackendResult res;
                        if (Backup != null && !(res = Backup.Run(ops).Result).Ok)
                            throw new TwoPCBackupFailedException(res);
                        return new BackendResult(true);
                    }).Result;
                });
        }

        /// <summary>
        /// Override should send prepare messages to all involved servers. Returned task should
        /// finish when the complete outcome is known.
        /// </summary>
        protected abstract Task<BackendResult> Prepare(Guid transactionId, IEnumerable<DataOp> ops);

        /// <summary>
        /// Override should send commit messages to all involved servers. Returned task should
        /// finish when the complete outcome is known.
        /// </summary>
        protected abstract Task Commit(Guid transactionId, IEnumerable<DataOp> ops);

        /// <summary>
        /// Override should send abort messages, and return without waiting for response.
        /// </summary>
        protected abstract void Abort(Guid transactionId, IEnumerable<DataOp> ops);

        public QueryResult<T> Query<T>(Query query) where T : DistributedBase, new()
        {
            if (Backup != null)
                return new Func<Query, QueryResult<T>>[] { q => Backup.Query<T>(q), q => DoQuery<T>(q) }
                    .QueryParallelSafe(f => f(query));
            return DoQuery<T>(query);
        }

        protected abstract QueryResult<T> DoQuery<T>(Query query) where T : DistributedBase, new();

        private readonly ShieldedDict<Guid, CommitContinuation> _transactions = new ShieldedDict<Guid, CommitContinuation>();

        /// <summary>
        /// Inheritors should call this when a prepare message is received.
        /// Returns true if transaction is valid.
        /// </summary>
        protected bool MsgPrepare(Guid transactionId, IEnumerable<DataOp> ops)
        {
            var cont = Repository.PrepareExtern(ops);
            if (cont != null)
            {
                if (Backup != null)
                    cont.InContext(() => Shield.SideEffect(() =>
                        Backup.Run(ops).Wait()));
                Shield.InTransaction(() => {
                    _transactions[transactionId] = cont;
                });
            }
            return cont != null;
        }

        private CommitContinuation GetAndRemove(Guid transactionId)
        {
            return Shield.InTransaction(() => {
                CommitContinuation c;
                if (_transactions.TryGetValue(transactionId, out c))
                {
                    _transactions.Remove(transactionId);
                    return c;
                }
                return null;
            });
        }

        /// <summary>
        /// Inheritors should call this when a commit message is received.
        /// Returns true if commit succeeds. It can only fail if the transaction
        /// ID is invalid, or if the transaction has already completed.
        /// </summary>
        protected bool MsgCommit(Guid transactionId)
        {
            var cont = GetAndRemove(transactionId);
            return cont != null && cont.TryCommit();
        }

        /// <summary>
        /// Inheritors should call this when an abort message is received.
        /// </summary>
        protected void MsgAbort(Guid transactionId)
        {
            var cont = GetAndRemove(transactionId);
            if (cont != null)
                cont.TryRollback();
        }
    }
}

