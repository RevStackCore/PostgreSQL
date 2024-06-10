using System;
using RevStackCore.DataAnnotations;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Reflection;
using Npgsql;
using Dapper;
using System.Linq;
using System.Threading.Tasks;
using static Microsoft.EntityFrameworkCore.DbLoggerCategory.Database;
using System.Text;

namespace RevStackCore.PostgreSQL
{
    public class PostgreSQLBulkClient<TEntity> where TEntity : class
    {
        private readonly string _connectionString;
        private readonly string _type;
        public PostgreSQLBulkClient(string connectionString)
        {
            _connectionString = connectionString;
            var entityType = typeof(TEntity);
            _type = entityType.Name;
            var tableAttribute = entityType.GetCustomAttribute<TableAttribute>(true);
            if (tableAttribute != null && !string.IsNullOrEmpty(tableAttribute.Name))
            {
                _type = tableAttribute.Name;
            }
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

        public async Task<int> BulkInsert(IEnumerable<TEntity> entities)
        {
            int insertedCount = 0;

            var entityType = typeof(TEntity);
            var properties = entityType.GetProperties(BindingFlags.Public | BindingFlags.Instance);

            if (properties.Length == 0)
                throw new ArgumentException("The entity type must have at least one public property.");

            var columnNames = string.Join(", ", properties.Select(p => p.Name.ToLower()));

            var connection = new NpgsqlConnection(_connectionString);

            using (NpgsqlConnection db = connection)
            {
                using (var writer = db.BeginBinaryImport($"COPY {_type} ({columnNames}) FROM STDIN (FORMAT BINARY)"))
                {
                    

                    foreach (var entity in entities)
                    {
                        await writer.StartRowAsync().ConfigureAwait(false);
                        foreach (var property in properties)
                        {
                            var value = property.GetValue(entity);
                            var npgsqlDbType = GetNpgsqlDbType(property.PropertyType);
                            await writer.WriteAsync(value, npgsqlDbType).ConfigureAwait(false);
                        }

                        insertedCount++;
                    }
                    await writer.CompleteAsync().ConfigureAwait(false);
                }
            }

            return insertedCount;
        }

        public async Task<int> BulkUpdate(IEnumerable<TEntity> entities)
        {
            int updatedCount = 0;

            var entityType = typeof(TEntity);
            var properties = entityType.GetProperties(BindingFlags.Public | BindingFlags.Instance).Where(p => p.Name != "id").ToArray();
            var keyProperty = entityType.GetProperty("id");

            if (properties.Length == 0 || keyProperty == null)
                throw new ArgumentException("The entity type must have at least one public property excluding the key column, and the key column must exist.");

            var columnNames = string.Join(", ", properties.Select(p => p.Name.ToLower()));
            var setClauses = string.Join(", ", properties.Select(p => $"{p.Name.ToLower()} = EXCLUDED.{p.Name.ToLower()}"));

            var tempTableName = $"temp_{Guid.NewGuid().ToString("N")}";

            var connection = new NpgsqlConnection(_connectionString);

            using (NpgsqlConnection db = connection)
            {
                using (var writer = db.BeginBinaryImport($"COPY {tempTableName} ({"id".ToLower()}, {columnNames}) FROM STDIN (FORMAT BINARY)"))
                {
                    foreach (var entity in entities)
                    {
                        await writer.StartRowAsync().ConfigureAwait(false);
                        await writer.WriteAsync(keyProperty.GetValue(entity), GetNpgsqlDbType(keyProperty.PropertyType)).ConfigureAwait(false);

                        foreach (var property in properties)
                        {
                            var value = property.GetValue(entity);
                            var npgsqlDbType = GetNpgsqlDbType(property.PropertyType);
                            await writer.WriteAsync(value, npgsqlDbType).ConfigureAwait(false);
                        }

                        updatedCount++;
                    }

                    await writer.CompleteAsync().ConfigureAwait(false);
                }

                var updateSql = new StringBuilder();
                updateSql.AppendLine($"CREATE TEMP TABLE {tempTableName} AS TABLE {_type} WITH NO DATA;");
                updateSql.AppendLine($"COPY {tempTableName} ({"id".ToLower()}, {columnNames}) FROM STDIN (FORMAT BINARY);");
                updateSql.AppendLine($"UPDATE {_type} SET {setClauses} FROM {tempTableName} WHERE {_type}.{"id".ToLower()} = {tempTableName}.{"id".ToLower()};");
                updateSql.AppendLine($"DROP TABLE {tempTableName};");

                using (var cmd = new NpgsqlCommand(updateSql.ToString(), db))
                {
                    await cmd.ExecuteNonQueryAsync().ConfigureAwait(false);
                }
            }

            return updatedCount;
        }

        public Task<int> BulkDelete()
        {
            var connection = new NpgsqlConnection(_connectionString);
            using (NpgsqlConnection db = connection)
            {
                string sql = "Delete From " + _type;
                var query = db.Execute(sql);
                return Task.FromResult(query);
            }
        }

        private NpgsqlTypes.NpgsqlDbType GetNpgsqlDbType(Type type)
        {
            if (type == typeof(string))
                return NpgsqlTypes.NpgsqlDbType.Varchar;
            if (type == typeof(int))
                return NpgsqlTypes.NpgsqlDbType.Integer;
            if (type == typeof(bool))
                return NpgsqlTypes.NpgsqlDbType.Boolean;
            if (type == typeof(DateTime))
                return NpgsqlTypes.NpgsqlDbType.Timestamp;
            if (type == typeof(double))
                return NpgsqlTypes.NpgsqlDbType.Double;
            // Add more type mappings as needed
            throw new NotSupportedException($"Type {type.Name} is not supported.");
        }
    }
}

