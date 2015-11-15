using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Text;
using System.Data;
using System.Diagnostics;
using System.Collections.Generic;
using Dapper;
using Shielded;

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

        public void Run(IEnumerable<SqlOp> p)
        {
            using (var conn = _connFactory())
            {
                conn.Open();
                using (var tran = conn.BeginTransaction(IsolationLevel.ReadCommitted))
                {
                    foreach (var op in p)
                        op(conn);
                    tran.Commit();
                }
            }
        }

        public static SqlOp Do(IEnumerable<SqlOp> ops)
        {
            return conn => {
                foreach (var op in ops)
                    op(conn);
            };
        }

        public ShieldedDict<TKey, T> LoadDict<T, TKey>() where T : class, IEntity<TKey>, new()
        {
            var name = typeof(T).Name;
            Debug.WriteLine("Loading dict {0}", (object)name);
            using (var conn = _connFactory())
                // this transaction cannot conflict, it's just creating new objects.
                return Shield.InTransaction(() =>
                    new ShieldedDict<TKey, T>(
                        conn.Query<T>(string.Format("select * from {0}", name))
                        .Select(t => new KeyValuePair<TKey, T>(t.Id, MapFromDb.Map(t)))));
        }

        public SqlOp Insert<T>(T entity) where T : IEntity
        {
            return conn => {
                Debug.WriteLine("Inserting entity {0}", entity);
                conn.Execute(_insertSqls.GetOrAdd(typeof(T), GetInsertSql), entity);
                entity.Inserted = true;
            };
        }

        public SqlOp Update<T>(T entity) where T : IEntity
        {
            return conn => {
                Debug.WriteLine("Updating entity {0}", entity);
                conn.Execute(_updateSqls.GetOrAdd(typeof(T), GetUpdateSql), entity);
            };
        }

        public SqlOp Delete<T, TKey>(TKey id) where T : IEntity<TKey>
        {
            return conn => {
                Debug.WriteLine("Deleting entity {0}[{1}]", typeof(T).Name, id);
                conn.Execute(string.Format("delete from {0} where Id = @id", typeof(T).Name),
                    new { id });
            };
        }

        #region Insert SQL

        string GetInsertSql(Type entityType)
        {
            var props = entityType.GetProperties().Where(pi => pi.Name != "Inserted");
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
                .Where(pi => pi.Name != "Id" && pi.Name != "Inserted");
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

