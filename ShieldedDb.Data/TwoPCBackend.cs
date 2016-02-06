using System;
using System.Threading.Tasks;
using System.Collections.Generic;
using System.Linq;
using Shielded;

namespace ShieldedDb.Data
{
    public class TwoPCFailedException : Exception
    {
        public bool FailedDuringCommit { get; set; }

        public TwoPCFailedException(bool failedInCommit, AggregateException inner)
            : base(null, inner)
        {
            FailedDuringCommit = failedInCommit;
        }
    }

    /// <summary>
    /// Base class for a backend which supports 2-phase commit via some kind of
    /// messages.
    /// </summary>
    public abstract class TwoPCBackend : IBackend
    {
        public Task<BackendResult> Run(IEnumerable<DataOp> ops)
        {
            var transId = Guid.NewGuid();
            return Prepare(transId, ops)
                .ContinueWith((Task<BackendResult> prepareTask) => {
                    if (prepareTask.Exception != null || !prepareTask.Result.Ok)
                    {
                        Abort(transId);
                        return prepareTask.Result;
                    }
                    return Commit(transId).ContinueWith(commitTask => {
                        if (commitTask.Exception != null)
                            throw commitTask.Exception;
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
        protected abstract Task Commit(Guid transactionId);

        /// <summary>
        /// Override should send abort messages, and return without waiting for response.
        /// </summary>
        protected abstract void Abort(Guid transactionId);

        public abstract IEnumerable<T> LoadAll<T>() where T : DistributedBase, new();

        private readonly ShieldedDict<Guid, CommitContinuation> _transactions = new ShieldedDict<Guid, CommitContinuation>();

        /// <summary>
        /// Inheritors should call this when a prepare message is received.
        /// Returns true if transaction is valid.
        /// </summary>
        protected bool MsgPrepare(Guid transactionId, IEnumerable<DataOp> ops)
        {
            var cont = Repository.PrepareExtern(ops);
            if (cont != null) Shield.InTransaction(() => { _transactions[transactionId] = cont; });
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

