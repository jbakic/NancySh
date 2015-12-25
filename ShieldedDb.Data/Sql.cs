using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Text;
using System.Data;
using System.Diagnostics;
using System.Collections.Generic;
using Dapper;
using Shielded;
using System.Threading.Tasks;

namespace ShieldedDb.Data
{
    internal delegate void SqlOp(IDbConnection conn);

    internal class Sql
    {
        readonly Func<IDbConnection> _connFactory;

        public Sql(Func<IDbConnection> connFactory)
        {
            _connFactory = connFactory;
        }

        public static readonly SqlOp Nop = conn => {};

        public Task<bool> Run(IEnumerable<DataOp> p)
        {
            return Task.Run(() => {
                using (var conn = _connFactory())
                {
                    conn.Open();
                    using (var tran = conn.BeginTransaction(IsolationLevel.ReadCommitted))
                    {
                        foreach (var op in p.Select(Convert))
                            op(conn);
                        tran.Commit();
                        return true;
                    }
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

        public static SqlOp Do(IEnumerable<SqlOp> ops)
        {
            return conn => {
                foreach (var op in ops)
                    op(conn);
            };
        }

        public T[] LoadAll<T>() where T : class, IDistributed, new()
        {
            var name = typeof(T).Name;
            Debug.WriteLine("Loading all {0}", (object)name);
            using (var conn = _connFactory())
                return conn.Query<T>(string.Format("select * from {0}", name)).ToArray();
        }

        public SqlOp Insert(IDistributed entity)
        {
            return conn => {
                Debug.WriteLine("Inserting entity {0}", entity);
                conn.Execute(_insertSqls.GetOrAdd(entity.GetType(), GetInsertSql), entity);
            };
        }

        public SqlOp Update(IDistributed entity)
        {
            return conn => {
                Debug.WriteLine("Updating entity {0}", entity);
                conn.Execute(_updateSqls.GetOrAdd(entity.GetType(), GetUpdateSql), entity);
            };
        }

        public SqlOp Delete(IDistributed entity)
        {
            var type = entity.GetType();
            return conn => {
                Debug.WriteLine("Deleting entity {0}[{1}]", type.Name, entity.IdValue);
                conn.Execute(string.Format("delete from {0} where Id = @Id", type.Name),
                    new { Id = entity.IdValue });
            };
        }

        #region Insert SQL

        string GetInsertSql(Type entityType)
        {
            var props = entityType.GetProperties();
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
                .Where(pi => pi.Name != "Id");
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

