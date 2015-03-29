using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Threading;
using ShieldedDb.Models;
using Dapper;
using Npgsql;
using Shielded;
using System.Text;
using System.Data;

namespace ShieldedDb.Data
{
    public class DataDeamon : IDisposable
    {
        class Op
        {
            public Action<IDbConnection> Execute;
        }

        readonly BlockingCollection<Op> _queue;
        readonly Thread _writer;
        readonly CancellationTokenSource _cancel;

        internal DataDeamon(string connectionString)
        {
            _queue = new BlockingCollection<Op>(20);
            _cancel = new CancellationTokenSource();

            var conn = new NpgsqlConnection(connectionString);
            _writer = new Thread(() => {
                try
                {
                    foreach (var op in _queue.GetConsumingEnumerable(_cancel.Token))
                    {
                        try
                        {
                            op.Execute(conn);
                        }
                        catch (Exception ex)
                        {
                            Console.Error.WriteLine("Exception in deamon! {0}", ex);
                        }
                    }
                }
                catch (OperationCanceledException) {}

                _queue.CompleteAdding();
                foreach (var op in _queue.GetConsumingEnumerable())
                    op.Execute(conn);

                conn.Dispose();
            });
            _writer.Start();
        }

        public void Dispose()
        {
            _cancel.Cancel();
        }

        void AddOp(Action<IDbConnection> exe)
        {
            _queue.Add(new Op { Execute = exe });
        }

        public void Insert<T>(T entity) where T : IEntity
        {
            AddOp(conn => {
                conn.Execute(_insertSqls.GetOrAdd(typeof(T), GetInsertSql), entity);
                Shield.InTransaction(
                    () => { entity.Saved = true; });
            });
        }

        public void Update<T>(T entity) where T : IEntity
        {
            AddOp(conn =>
                conn.Execute(_updateSqls.GetOrAdd(typeof(T), GetUpdateSql), entity));
        }

        public void Delete<T, TKey>(TKey id) where T : IEntity<TKey>
        {
            AddOp(conn => conn.Execute(
                string.Format("delete from {0} where Id = @id", typeof(T).Name),
                new { id }));
        }

        #region Insert SQL

        string GetInsertSql(Type entityType)
        {
            var props = entityType.GetProperties().Where(pi => pi.Name != "Saved");
            StringBuilder sb = new StringBuilder();
            sb.Append("insert into ");
            sb.Append(entityType.Name);
            sb.Append(" (");
            sb.Append(string.Join(", ", props.Select(p => p.Name)));
            sb.Append(") values (");
            sb.Append(string.Join(", ", props.Select(p => "@" + p.Name)));
            sb.Append(")");
            return sb.ToString();
        }

        static readonly ConcurrentDictionary<Type, string> _insertSqls =
            new ConcurrentDictionary<Type, string>();

        #endregion

        #region Update SQL

        string GetUpdateSql(Type entityType)
        {
            var props = entityType.GetProperties()
                .Where(pi => pi.Name != "Id" && pi.Name != "Saved");
            StringBuilder sb = new StringBuilder();
            sb.Append("update ");
            sb.Append(entityType.Name);
            sb.Append(" set ");
            sb.Append(string.Join(", ", props.Select(p => p.Name + " = @" + p.Name)));
            sb.Append(" where Id = @Id");
            return sb.ToString();
        }

        static readonly ConcurrentDictionary<Type, string> _updateSqls =
            new ConcurrentDictionary<Type, string>();

        #endregion
    }
}

