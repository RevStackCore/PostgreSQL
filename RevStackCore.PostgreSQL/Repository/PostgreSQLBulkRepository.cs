using System;
using RevStackCore.Extensions.SQL;
using RevStackCore.Pattern;
using RevStackCore.Pattern.SQL;
using RevStackCore.SQL.Client;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Linq.Expressions;
using RevStackCore.PostgreSQL.DbContext;
using Npgsql;

namespace RevStackCore.PostgreSQL.Repository
{
    public class PostgreSQLBulkRepository<TEntity, TKey> : IBulkRepository<TEntity, TKey> where TEntity : class, IEntity<TKey>
    {
        private readonly TypedClient<TEntity, NpgsqlConnection, TKey> _typedClient;
        private readonly PostgreSQLBulkClient<TEntity> _bulkClient;
        public PostgreSQLBulkRepository(PostgreSQLDbContext context)
        {
            _typedClient = new TypedClient<TEntity, NpgsqlConnection, TKey>(context.ConnectionString, SQLLanguageType.MySQL);
            _bulkClient = new PostgreSQLBulkClient<TEntity>(context.ConnectionString);
        }

        public TEntity Add(TEntity entity)
        {
            return _typedClient.Insert(entity);
        }

        public int BulkInsert(IEnumerable<TEntity> entities)
        {
            return _bulkClient.BulkInsert(entities).Result;
        }

        public int BulkUpdate(IEnumerable<TEntity> entities)
        {
            return _bulkClient.BulkUpdate(entities).Result;
        }

        public int BulkDelete()
        {
            return _bulkClient.BulkDelete().Result;
        }

        public void Delete(TEntity entity)
        {
            _typedClient.Delete(entity);
        }

        public IQueryable<TEntity> Find(Expression<Func<TEntity, bool>> predicate)
        {
            return _typedClient.Find(predicate);
        }

        public IEnumerable<TEntity> Get()
        {
            return _typedClient.GetAll();
        }

        public TEntity GetById(TKey id)
        {
            return _typedClient.GetById(id);
        }

        public TEntity Update(TEntity entity)
        {
            return _typedClient.Update(entity);
        }

        public IDbConnection Db
        {
            get
            {
                return _typedClient.Db;
            }
        }
    }
}

