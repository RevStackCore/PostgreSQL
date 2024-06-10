using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics.SymbolStore;
using System.Linq;
using System.Linq.Expressions;
using Dapper;
using Npgsql;
using RevStackCore.Extensions.SQL;
using RevStackCore.Pattern;
using RevStackCore.Pattern.SQL;
using RevStackCore.PostgreSQL.DbContext;
using RevStackCore.SQL.Client;

namespace RevStackCore.PostgreSQL.Repository
{
    public class PostgreSQLRepository<TEntity, TKey> : ISQLRepository<TEntity, TKey> where TEntity : class, IEntity<TKey>
    {
        private readonly TypedClient<TEntity, NpgsqlConnection, TKey> _typedClient;
        public PostgreSQLRepository(PostgreSQLDbContext context)
        {
            _typedClient = new TypedClient<TEntity, NpgsqlConnection, TKey>(context.ConnectionString, SQLLanguageType.PostgreSQL);
        }

        public TEntity Add(TEntity entity)
        {
            return _typedClient.Insert(entity);
        }

        public void Delete(TEntity entity)
        {
            _typedClient.Delete(entity);
        }

        public DynamicParameters Execute(string sp_procedure, DynamicParameters param)
        {
            return _typedClient.Execute(sp_procedure, param);
        }

        public void Execute(string sp_procedure, object param)
        {
            _typedClient.Execute(sp_procedure, param);
        }

        public void Execute(string sp_procedure)
        {
            _typedClient.Execute(sp_procedure);
        }

        public TResult ExecuteFunction<TResult>(string s_function, object param) where TResult : class
        {
            return _typedClient.ExecuteFunction<TResult>(s_function, param);
        }

        public TResult ExecuteFunction<TResult>(string s_function) where TResult : class
        {
            return _typedClient.ExecuteFunction<TResult>(s_function);
        }

        public IEnumerable<TResult> ExecuteFunctionWithResults<TResult>(string s_function) where TResult : class
        {
            return _typedClient.ExecuteFunctionWithResults<TResult>(s_function);
        }

        public IEnumerable<TResult> ExecuteFunctionWithResults<TResult>(string s_function, object param) where TResult : class
        {
            return _typedClient.ExecuteFunctionWithResults<TResult>(s_function, param);
        }

        public IEnumerable<TResult> ExecuteProcedure<TResult>(string sp_procedure, object param) where TResult : class
        {
            return _typedClient.ExecuteProcedure<TResult>(sp_procedure, param);
        }

        public IEnumerable<TResult> ExecuteProcedure<TResult>(string sp_procedure) where TResult : class
        {
            return _typedClient.ExecuteProcedure<TResult>(sp_procedure);
        }

        public TResult ExecuteProcedureSingle<TResult>(string sp_procedure, object param) where TResult : class
        {
            return _typedClient.ExecuteProcedureSingle<TResult>(sp_procedure, param);
        }

        public TResult ExecuteProcedureSingle<TResult>(string sp_procedure) where TResult : class
        {
            return _typedClient.ExecuteProcedureSingle<TResult>(sp_procedure);
        }

        public TValue ExecuteScalar<TValue>(string s_function, object param) where TValue : struct
        {
            return _typedClient.ExecuteScalar<TValue>(s_function, param);
        }

        public TValue ExecuteScalar<TValue>(string s_function) where TValue : struct
        {
            return _typedClient.ExecuteScalar<TValue>(s_function);
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

