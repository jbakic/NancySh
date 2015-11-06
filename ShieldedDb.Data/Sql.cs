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
    public class Sql
    {
        readonly string _connString;

        internal Sql(string connectionString)
        {
            _connString = connectionString;
        }

        public ShieldedDict<TKey, T> LoadDict<T, TKey>() where T : class, IEntity<TKey>, new()
        {
            var name = typeof(T).Name;
            Debug.WriteLine("Loading dict {0}", name);
            using (var conn = new NpgsqlConnection(_connString))
            {
                // this transaction cannot conflict, it's just creating new objects.
                return Shield.InTransaction(() =>
                    new ShieldedDict<TKey, T>(
                        conn.Query<T>(string.Format("select * from {0}", name))
                        .Select(t => new KeyValuePair<TKey, T>(t.Id, MapFromDb.Map(t)))));
            }
        }

        public void Insert<T>(T entity) where T : IEntity
        {
            Debug.WriteLine("Inserting entity {0}", entity);
            using (var conn = new NpgsqlConnection(_connString))
                conn.Execute(_insertSqls.GetOrAdd(typeof(T), GetInsertSql), entity);
            entity.Saved = true;
        }

        public void Update<T>(T entity) where T : IEntity
        {
            Debug.WriteLine("Updating entity {0}", entity);
            using (var conn = new NpgsqlConnection(_connString))
                conn.Execute(_updateSqls.GetOrAdd(typeof(T), GetUpdateSql), entity);
        }

        public void Delete<T, TKey>(TKey id) where T : IEntity<TKey>
        {
            Debug.WriteLine("Deleting entity {0}[{1}]", typeof(T).Name, id);
            using (var conn = new NpgsqlConnection(_connString))
                conn.Execute(string.Format("delete from {0} where Id = @id", typeof(T).Name),
                    new { id });
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

