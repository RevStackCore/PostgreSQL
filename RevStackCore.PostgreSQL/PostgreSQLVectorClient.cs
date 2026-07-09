using System;
using Npgsql;
using System.Data;
using System.Threading.Tasks;
using Dapper;
using System.Collections;
using System.Collections.Generic;
using Newtonsoft.Json;
using System.IO;
using System.Text;
using System.Reflection;
using Newtonsoft.Json.Linq;
using System.Linq;
using RevStackCore.Extensions.SQL;

namespace RevStackCore.PostgreSQL
{
    public class PostgreSQLVectorClient
    {
        private readonly string _connectionString;
        private readonly string _tenant;

        private List<string> _collections = new List<string>();

        public PostgreSQLVectorClient(string tenant, string connectionString)
        {
            _connectionString = connectionString;
            _tenant = tenant;

            Init();
        }

        public PostgreSQLVectorClient(string connectionString)
        {
            _connectionString = connectionString;
            _tenant = "";

            Init();
        }

        public void Init()
        {
            var collections = GetCollectionsAsync().Result;
            _collections.AddRange(collections);
        }

        public IDbConnection Db
        {
            get
            {
                var connection = new NpgsqlConnection(_connectionString);
                IDbConnection db = connection;
                return db;
            }
        }

        public async Task<IList<string>> GetCollectionsAsync()
        {
            var tableNames = new List<string>();

            try
            {
                string sql = @"SELECT table_name
                       FROM information_schema.tables
                       WHERE table_schema = '" + _tenant + "' AND table_name LIKE 'vector_%'";

                using (var conn = new NpgsqlConnection(_connectionString))
                {
                    conn.Open();

                    using (var cmd = new NpgsqlCommand(sql, conn))
                    using (var reader = cmd.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            tableNames.Add(reader.GetString(0));
                        }
                    }
                } 
            }
            catch (Exception ex)
            {

            }

            return await Task.FromResult(tableNames);
        }

        public async Task BulkInsertAsync(string collectionName, JArray array, bool refresh)
        {
            if (refresh)
            {
                await DeleteAllAsync(collectionName);
            }

            await BulkInsertAsync(collectionName, array);
        }

        public async Task BulkInsertAsync(string collectionName, JArray array)
        {
            var entities = new List<VectorStoreEntity<object>>();

            foreach (var item in array)
            {
                var entity = (JObject)item;
                var entityMetadata = JsonConvert.SerializeObject(entity);
                var data = new List<string>() { entity.ToString() };

                if (!entity.ContainsKey("id"))
                {
                    entity["id"] = Guid.NewGuid().ToString();
                }

                var id = entity["id"].ToString();
                var text = "";

                foreach (JProperty property in entity.Properties())
                {
                    string name = property.Name;
                    JToken value = property.Value;
                    text += $"{name}: {value} ";
                }

                var vectorStoreEntity = new VectorStoreEntity<object>
                {
                    Id = id,
                    Text = text,
                    AdditionalMetadata = entityMetadata
                };

                entities.Add(vectorStoreEntity);
            }

            await BulkInsertAsync(collectionName, entities);
        }

        public async Task BulkInsertAsync(string collectionName, IEnumerable<VectorStoreEntity<object>> entities)
        {
            if (!collectionName.StartsWith("vector_"))
            {
                collectionName = "vector_" + collectionName;
            }

            if (!_collections.Contains(collectionName))
            {
                await this.CreateCollectionAsync(collectionName);
            }

            if (!string.IsNullOrEmpty(_tenant))
            {
                collectionName = _tenant + "." + collectionName;
            }

            using (var connection = new NpgsqlConnection(_connectionString))
            {
                await connection.OpenAsync();
                try
                {
                    Console.WriteLine(DateTime.Now.ToString() + "Start BulkInsertAsync: collectionName=" + collectionName + " count=" + entities.Count());

                    foreach (var batch in entities.Batch(5000))
                    {
                        using (var writer = connection.BeginBinaryImport($"COPY {collectionName} (id, text, additional_metadata) FROM STDIN (FORMAT BINARY)"))
                        {
                            foreach (var entity in batch)
                            {
                                writer.StartRow();
                                writer.Write(entity.Id, NpgsqlTypes.NpgsqlDbType.Text);
                                writer.Write(entity.Text, NpgsqlTypes.NpgsqlDbType.Text);
                                writer.Write(entity.AdditionalMetadata?.ToString(), NpgsqlTypes.NpgsqlDbType.Text);
                            }
                            writer.Complete();
                            writer.Close();
                            writer.Dispose();
                        }
                    }

                    Console.WriteLine(DateTime.Now.ToString() + "Finished BulkInsertAsync: collectionName=" + collectionName);
                }
                finally
                {
                    if (connection.State == ConnectionState.Open)
                    {
                        connection.Close();
                    }
                    await connection.DisposeAsync();
                }
            }

            /*using (var connection = new NpgsqlConnection(_connectionString))
            {
                await connection.OpenAsync();

                using (var writer = connection.BeginBinaryImport($"COPY {collectionName} (id, text, additional_metadata) FROM STDIN (FORMAT BINARY)"))
                {
                    try
                    {

                        foreach (var entity in entities)
                        {
                            writer.StartRow();
                            writer.Write(entity.Id, NpgsqlTypes.NpgsqlDbType.Text);
                            writer.Write(entity.Text, NpgsqlTypes.NpgsqlDbType.Text);
                            //writer.Write(entity.Description, NpgsqlTypes.NpgsqlDbType.Text);
                            writer.Write(entity.AdditionalMetadata, NpgsqlTypes.NpgsqlDbType.Text);
                        }
                        writer.Complete();
                    }
                    finally
                    {
                        await writer.DisposeAsync();

                        if (connection.State == ConnectionState.Open)
                            connection.Close();

                        await connection.DisposeAsync();
                    }
                }
            }*/
        }

        public async Task<IList<VectorStoreEntity<object>>> SearchAsync(string collectionName, string query, string filter, int limit)
        {
            //var entities = new List<VectorStoreEntity<object>>();

            if (!collectionName.StartsWith("vector_"))
            {
                collectionName = "vector_" + collectionName;
            }

            var orderBy = ""; //" ORDER BY distance, rank desc;";

            var where = "";

            if (!string.IsNullOrEmpty(_tenant))
            {
                collectionName = _tenant + "." + collectionName;
            }

            if (!string.IsNullOrEmpty(filter))
            {
                where = " WHERE text @@ to_tsquery('english', '" + filter + "')"; // 'table & chair'
            }

            var sql = @"WITH query AS (
                        SELECT pgml.embed('intfloat/e5-small-v2', '" + query + @"') AS embedding
                    )
                    SELECT DISTINCT
                            id,
                            text,
                            additional_metadata as additionalMetadata,
                            pgml.distance_l2(query.embedding, " + collectionName + @".embedding) as distance
                    FROM " + collectionName + @", query
                    ORDER BY distance limit " + limit + @";";

            sql += where + " " + orderBy;

            var results = await this.Db.QueryAsync<VectorStoreEntity<object>>(sql);

            return results.ToList();
        }

        public async Task<IList<TEntity>> SearchAsync<TEntity>(string collectionName, string query, string filter, int limit)
        {
            var entities = new List<TEntity>();

            /*if (!collectionName.StartsWith("vector_"))
            {
                collectionName = "vector_" + collectionName;
            }

            var orderBy = ""; //" ORDER BY distance, rank desc;";

            var where = "";

            if (!string.IsNullOrEmpty(_tenant))
            {
                collectionName = _tenant + "." + collectionName;
            }

            if (!string.IsNullOrEmpty(filter))
            {
                where = " WHERE text @@ to_tsquery('english', '" + filter + "')"; // 'table & chair'
            }

            var sql = @"WITH query AS (
                        SELECT pgml.embed('intfloat/e5-small-v2', '" + query + @"') AS embedding
                    )
                    SELECT DISTINCT
                            id,
                            text,
                            additional_metadata as additionalMetadata,
                            pgml.distance_l2(query.embedding, " + collectionName + @".embedding) as distance
                    FROM " + collectionName + @", query
                    ORDER BY distance limit "  + limit + @";";

            sql += where + " " + orderBy;

            var results = await this.Db.QueryAsync<VectorStoreEntity<object>>(sql);
            */

            var results = await SearchAsync(collectionName, query, filter, limit);

            foreach (var result in results)
            {
                // unpack entity from metadata
                var entity = JsonConvert.DeserializeObject<TEntity>(result.AdditionalMetadata);

                if (entity != null)
                {
                    entities.Add(entity);
                }
            }

            return entities;
        }

        public List<IVectorStoreEntity<object>> Get(string collectionName)
        {
            if (!collectionName.StartsWith("vector_"))
            {
                collectionName = "vector_" + collectionName;
            }

            if (!string.IsNullOrEmpty(_tenant))
            {
                collectionName = _tenant + "." + collectionName;
            }

            string query = "SELECT id, text, additional_metadata, embedding FROM " + collectionName;

            List<IVectorStoreEntity<object>> results = new List<IVectorStoreEntity<object>>();

            using (var connection = new NpgsqlConnection(_connectionString))
            {
                connection.Open();

                using (var command = new NpgsqlCommand(query, connection))
                {
                    //command.Parameters.AddWithValue("@id", id);

                    using (var reader = command.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            results.Add(new VectorStoreEntity<object>
                            {
                                Id = reader.GetString(0),
                                Text = reader.GetString(1),
                                //Description = reader.IsDBNull(2) ? null : reader.GetString(2),
                                AdditionalMetadata = reader.GetString(2),
                                Embedding = reader[4] as float[]
                            });
                        }
                    }
                }
            }

            return results;
        }

        public IVectorStoreEntity<object> GetById(string collectionName, object id)
        {
            if (!collectionName.StartsWith("vector_"))
            {
                collectionName = "vector_" + collectionName;
            }

            if (!string.IsNullOrEmpty(_tenant))
            {
                collectionName = _tenant + "." + collectionName;
            }

            string query = "SELECT id, text, additional_metadata, embedding FROM " + collectionName + " WHERE id = @id LIMIT 1";

            using (var connection = new NpgsqlConnection(_connectionString))
            {
                connection.Open();

                using (var command = new NpgsqlCommand(query, connection))
                {
                    command.Parameters.AddWithValue("@id", id);

                    using (var reader = command.ExecuteReader())
                    {
                        if (reader.Read())
                        {
                            return new VectorStoreEntity<object>
                            {
                                Id = reader.GetString(0),
                                Text = reader.GetString(1),
                                //Description = reader.IsDBNull(2) ? null : reader.GetString(2),
                                AdditionalMetadata = reader.GetString(2),
                                Embedding = reader[4] as float[]
                            };
                        }
                        else
                        {
                            return null; // or throw an exception if preferred
                        }
                    }
                }
            }
        }

        public async Task CreateCollectionAsync(string collectionName)
        {
            try
            {
                
                if (!collectionName.StartsWith("vector_"))
                {
                    collectionName = "vector_" + collectionName;
                }

                var tableName = collectionName;

                if (!string.IsNullOrEmpty(_tenant))
                {
                    collectionName = _tenant + "." + collectionName;
                }

                var sql = @"CREATE TABLE " + collectionName + @" (
                          id TEXT, 
                          text TEXT,
                          additional_metadata TEXT,
                          embedding FLOAT[] GENERATED ALWAYS AS (pgml.normalize_l2(pgml.embed('intfloat/e5-small-v2', text))) STORED
                        );";

                var connection = new NpgsqlConnection(_connectionString);

                using (NpgsqlConnection db = connection)
                {
                    var query = db.Execute(sql);
                }

                _collections.Add(tableName);
            }
            catch (Exception ex)
            {

            }

            await Task.CompletedTask;
        }

        public async Task DeleteCollectionAsync(string collectionName)
        {
            try
            {
                if (!collectionName.StartsWith("vector_"))
                {
                    collectionName = "vector_" + collectionName;
                }

                var tableName = collectionName;

                if (!string.IsNullOrEmpty(_tenant))
                {
                    collectionName = _tenant + "." + collectionName + ";";
                }

                var connection = new NpgsqlConnection(_connectionString);

                using (NpgsqlConnection db = connection)
                {
                    string sql = "DROP TABLE IF EXISTS " + collectionName;
                    var query = db.Execute(sql);
                }

                _collections.Remove(tableName);
            }
            catch (Exception ex)
            {

            }

            await Task.CompletedTask;
        }

        public async Task DeleteAsync(string collectionName, string id)
        {
            try
            {
                if (!collectionName.StartsWith("vector_"))
                {
                    collectionName = "vector_" + collectionName;
                }

                if (!string.IsNullOrEmpty(_tenant))
                {
                    collectionName = _tenant + "." + collectionName + ";";
                }

                var connection = new NpgsqlConnection(_connectionString);

                using (NpgsqlConnection db = connection)
                {
                    string sql = "DELETE FROM " + collectionName + " WHERE id = '" + id + "'";
                    var query = db.Execute(sql);
                }
            }
            catch (Exception ex)
            {

            }

            await Task.CompletedTask;
        }

        public async Task DeleteAllAsync(string collectionName)
        {
            try
            {
                if (!collectionName.StartsWith("vector_"))
                {
                    collectionName = "vector_" + collectionName;
                }

                if (!string.IsNullOrEmpty(_tenant))
                {
                    collectionName = _tenant + "." + collectionName + ";";
                }

                var connection = new NpgsqlConnection(_connectionString);

                using (NpgsqlConnection db = connection)
                {
                    string sql = "DELETE FROM " + collectionName;
                    var query = db.Execute(sql);
                }
            }
            catch (Exception ex)
            {

            }

            await Task.CompletedTask;
        }
    }
}

