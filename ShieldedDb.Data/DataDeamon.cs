using System;
using System.Collections.Concurrent;
using System.Threading;
using ShieldedDb.Models;
using Dapper;
using Npgsql;
using Shielded;

namespace ShieldedDb.Data
{
    public class DataDeamon : IDisposable
    {
        class Op
        {
            public Type EntityType;
            public object Id;
            public Action Execute;
        }

        BlockingCollection<Op> _queue;
        NpgsqlConnection _conn;
        Thread _writer;
        CancellationTokenSource _cancel;

        internal DataDeamon(string connectionString)
        {
            _conn = new NpgsqlConnection(connectionString);
            _queue = new BlockingCollection<Op>(20);
            _cancel = new CancellationTokenSource();

            _writer = new Thread(() => {
                try
                {
                    foreach (var op in _queue.GetConsumingEnumerable(_cancel.Token))
                        op.Execute();
                }
                catch {}

                _queue.CompleteAdding();
                foreach (var op in _queue.GetConsumingEnumerable())
                    op.Execute();
            });
            _writer.Start();
        }

        public void Dispose()
        {
            _cancel.Cancel();
        }

        public void Insert(Test entity)
        {
            AddOp<Test, int>(entity, () => {
                _conn.Execute("insert into test (Id, Val) values (@Id, @Val)", entity);
                Shield.InTransaction(
                    () => { entity.Saved = true; });
            });
        }

        public void Delete<T, TKey>(T entity) where T : IEntity<TKey>
        {
            AddOp<T, TKey>(entity, () => _conn.Execute(
                string.Format("delete from {0} where Id = @Id", typeof(T).Name),
                new { Id = entity.Id }));
        }


        void AddOp<T, TKey>(T entity, Action exe) where T : IEntity<TKey>
        {
            if (!_queue.TryAdd(new Op {
                EntityType = typeof(T),
                Id = entity.Id,
                Execute = exe }))
            {
                throw new ApplicationException("Too busy right now, sorry.");
            }
        }
    }
}

