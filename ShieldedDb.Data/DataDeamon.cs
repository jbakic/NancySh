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
using System.Diagnostics;
using System.Collections.Generic;

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
                }
                finally
                {
                    conn.Dispose();
                }
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

        void BlockForOp(Action<IDbConnection> exe)
        {
            var waitCancel = new ManualResetEventSlim();
            AddOp(conn => {
                try
                {
                    exe(conn);
                }
                finally
                {
                    waitCancel.Set();
                }
            });
            waitCancel.Wait();
        }

        /// <summary>
        /// The deamon does the loading on his own thread, to avoid triggering
        /// UPDATE commands for the entities. If this is called from a transaction,
        /// that transaction should rollback to be able to read the dictionary.
        /// </summary>
        public void LoadDict<T, TKey>(Shielded<ShieldedDict<TKey, T>> dict) where T : class, IEntity<TKey>, new()
        {
            BlockForOp(conn => {
                string name = typeof(T).Name;
                Debug.WriteLine("Selecting entities {0}", (object)name);
                Shield.InTransaction(() => {
                    if (dict.Value != null)
                        return;
                    dict.Value = new ShieldedDict<TKey, T>(
                        conn.Query<T>(string.Format("select * from {0}", name))
                        .Select(t => new KeyValuePair<TKey, T>(t.Id, MapFromDb.Map(t))));
                    Database.RegisterDictionary(dict.Value);
                });
            });
        }

        public void Insert<T>(T entity) where T : IEntity
        {
            AddOp(conn => {
                Shield.InTransaction(() => {
                    Debug.WriteLine("Inserting entity {0}", entity);
                    conn.Execute(_insertSqls.GetOrAdd(typeof(T), GetInsertSql), entity);
                });
                // split, so the writing transaction (above) is never retried.
                Shield.InTransaction(
                    () => { entity.Saved = true; });
            });
        }

        public void Update<T>(T entity) where T : IEntity
        {
            AddOp(conn => Shield.InTransaction(() => {
                Debug.WriteLine("Updating entity {0}", entity);
                conn.Execute(_updateSqls.GetOrAdd(typeof(T), GetUpdateSql), entity);
            }));
        }

        public void Delete<T, TKey>(TKey id) where T : IEntity<TKey>
        {
            AddOp(conn => {
                Debug.WriteLine("Deleting entity {0}[{1}]", typeof(T).Name, id);
                conn.Execute(string.Format("delete from {0} where Id = @id", typeof(T).Name),
                    new { id });
            });
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

