using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Text;
using System.Data;
using System.Diagnostics;
using System.Collections.Generic;
using Dapper;
using System.Threading;
using System.Threading.Tasks;

namespace ShieldedDb.Data
{
    internal delegate void SqlOp(IDbConnection conn);

    public class SqlBackend : IBackend
    {
        readonly Func<IDbConnection> _connFactory;

        public SqlBackend(Func<IDbConnection> connFactory)
        {
            _connFactory = connFactory;
        }

        public Task<BackendResult> Run(IEnumerable<DataOp> p)
        {
            return Task.Run(() => {
                using (var conn = _connFactory())
                try
                {
                    conn.Open();
                    using (var tran = conn.BeginTransaction(IsolationLevel.ReadCommitted))
                    {
                        foreach (var op in p.Select(Convert))
                            op(conn);
                        tran.Commit();
                        return new BackendResult(true);
                    }
                }
                catch
                {
                    return new BackendResult(p);
                }
            });
        }

        private SqlOp Convert(DataOp op)
        {
            switch (op.OpType)
            {
            case DataOpType.Insert:
                return Insert(op.Entity);
            case DataOpType.Update:
                return Update(op.Entity);
            case DataOpType.Delete:
                return Delete(op.Entity);
            default:
                throw new NotSupportedException();
            }
        }

        public QueryResult<T> Query<T>(Query query) where T : DistributedBase, new()
        {
            var name = typeof(T).Name;
            Debug.WriteLine("Loading all {0}", (object)name);
            using (var conn = _connFactory())
                return new QueryResult<T>(true,
                    conn.Query<T>(string.Format("select * from {0}", name))
                    .Where(query.Check)
                    .ToArray());
        }

        private SqlOp Insert(DistributedBase entity)
        {
            return conn => {
                Debug.WriteLine("Inserting entity {0}", entity);
                conn.Execute(_insertSqls.GetOrAdd(entity.GetType(), GetInsertSql), entity);
            };
        }

        private SqlOp Update(DistributedBase entity)
        {
            return conn => {
                Debug.WriteLine("Updating entity {0}", entity);
                conn.Execute(_updateSqls.GetOrAdd(entity.GetType(), GetUpdateSql), entity);
            };
        }

        private SqlOp Delete(DistributedBase entity)
        {
            return conn => {
                var type = entity.GetType();
                Debug.WriteLine("Deleting entity {0}[{1}]", type.Name, entity.IdValue);
                conn.Execute(string.Format("delete from {0} where Id = @Id", type.Name),
                    new { Id = entity.IdValue });
            };
        }

        #region Insert SQL

        string GetInsertSql(Type entityType)
        {
            var props = entityType.GetProperties()
                .Where(pi => pi.Name != "IdValue");
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
                .Where(pi => pi.Name != "Id" && pi.Name != "IdValue");
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

