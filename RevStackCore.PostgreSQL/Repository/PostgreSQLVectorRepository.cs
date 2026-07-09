using System;
using Newtonsoft.Json.Linq;
using System.Threading.Tasks;
using System.Collections.Generic;
using RevStackCore.PostgreSQL.DbContext;
using RevStackCore.DataAnnotations;
using System.Data;
using System.Reflection;
using Npgsql.Internal;
using Newtonsoft.Json;
using RevStackCore.Pattern;
using static Dapper.SqlMapper;
using RevStackCore.SQL.Client;
using RevStackCore.Pattern.SQL;

namespace RevStackCore.PostgreSQL.Repository
{
	public class PostgreSQLVectorRepository<TEntity, TKey> : IVectorStoreRepository<TEntity, TKey> where TEntity : class, IEntity<TKey> //where TEntity : class, IEntity<TKey>
    {
        private readonly PostgreSQLVectorClient _vectorClient;
        //private readonly string? _type;

        public PostgreSQLVectorRepository(PostgreSQLDbContext context)
		{
            /*var entityType = typeof(TEntity);
            var _type = entityType.Name;
            var tableAttribute = entityType.GetCustomAttribute<TableAttribute>(true);
            if (tableAttribute != null && !string.IsNullOrEmpty(tableAttribute.Name))
            {
                _type = tableAttribute.Name;
            }
            */

            _vectorClient = new PostgreSQLVectorClient(context.Tenant, context.ConnectionString);
        }

        public async Task<IList<TEntity>> SearchAsync(string query, string filter, int limit)
        {
            var entityType = typeof(TEntity);
            var _type = entityType.Name;
            var tableAttribute = entityType.GetCustomAttribute<TableAttribute>(true);
            if (tableAttribute != null && !string.IsNullOrEmpty(tableAttribute.Name))
            {
                _type = tableAttribute.Name;
            }

            var entities = new List<TEntity>();

            return await _vectorClient.SearchAsync<TEntity>(_type, query, filter, limit);
        }

        public async Task<IEnumerable<TEntity>> GetAsync()
        {
            var entityType = typeof(TEntity);
            var _type = entityType.Name;
            var tableAttribute = entityType.GetCustomAttribute<TableAttribute>(true);
            if (tableAttribute != null && !string.IsNullOrEmpty(tableAttribute.Name))
            {
                _type = tableAttribute.Name;
            }

            var entities = new List<TEntity>();

            var results = _vectorClient.Get(_type);

            foreach (var result in results)
            {
                var entity = JsonConvert.DeserializeObject<TEntity>(result.AdditionalMetadata);

                if (entity != null)
                {
                    entities.Add(entity);
                }
            }

            return await Task.FromResult(entities);
        }

        public async Task<TEntity?> GetById(TKey id)
        {
            var entityType = typeof(TEntity);
            var _type = entityType.Name;
            var tableAttribute = entityType.GetCustomAttribute<TableAttribute>(true);
            if (tableAttribute != null && !string.IsNullOrEmpty(tableAttribute.Name))
            {
                _type = tableAttribute.Name;
            }

            if (id == null)
            {
                throw new ArgumentNullException("id is required");
            }

            var result = _vectorClient.GetById(_type, id);

            if (result == null)
            {
                return null;
            }

            var entity = JsonConvert.DeserializeObject<TEntity>(result.AdditionalMetadata);

            if (entity == null)
            {
                return null;
            }

            return await Task.FromResult(entity);
        }

        public async Task RefreshAsync(IEnumerable<TEntity> entities)
        {
            var entityType = typeof(TEntity);
            var _type = entityType.Name;
            var tableAttribute = entityType.GetCustomAttribute<TableAttribute>(true);
            if (tableAttribute != null && !string.IsNullOrEmpty(tableAttribute.Name))
            {
                _type = tableAttribute.Name;
            }

            var collections = await _vectorClient.GetCollectionsAsync();

            if (!collections.Contains("vector_" + _type))
            {
                await _vectorClient.CreateCollectionAsync(_type);
            }

            await _vectorClient.DeleteAllAsync(_type);
            //
            await this.InsertAsync(entities);
        }

        public async Task InsertAsync(IEnumerable<TEntity> entities)
        {
            var entityType = typeof(TEntity);
            var _type = entityType.Name;
            var tableAttribute = entityType.GetCustomAttribute<TableAttribute>(true);
            if (tableAttribute != null && !string.IsNullOrEmpty(tableAttribute.Name))
            {
                _type = tableAttribute.Name;
            }

            var list = new List<VectorStoreEntity<object>>();

            var properties = entityType.GetProperties(BindingFlags.Public | BindingFlags.Instance);

            if (properties.Length == 0)
                throw new ArgumentException("The entity type must have at least one public property.");

            foreach (var entity in entities)
            {
                var id = "";
                var text = "";
                var entityMetadata = JsonConvert.SerializeObject(entity);

                foreach (var property in properties)
                {
                    string name = property.Name;
                    var value = property.GetValue(entity);
                    text += $"{name}: {value} ";

                    if (name.ToLower() == "id")
                    {
                        id = value.ToString();
                    }
                }

                var vectorStoreEntity = new VectorStoreEntity<object>();
                vectorStoreEntity.Id = id;
                vectorStoreEntity.Text = text;
                //vectorStoreEntity.Description = "";
                vectorStoreEntity.AdditionalMetadata = entityMetadata;

                list.Add(vectorStoreEntity);
            }

            await _vectorClient.BulkInsertAsync(_type, list);
        }

        public async Task DeleteAsync(TEntity entity)
        {
            var entityType = typeof(TEntity);
            var _type = entityType.Name;
            var tableAttribute = entityType.GetCustomAttribute<TableAttribute>(true);
            if (tableAttribute != null && !string.IsNullOrEmpty(tableAttribute.Name))
            {
                _type = tableAttribute.Name;
            }

            await _vectorClient.DeleteAsync(_type, entity.Id.ToString());
        }

    }
}

