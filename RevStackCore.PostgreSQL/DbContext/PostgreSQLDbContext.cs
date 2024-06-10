using System;
namespace RevStackCore.PostgreSQL.DbContext
{
    public class PostgreSQLDbContext
    {
        public string ConnectionString { get; }
        public PostgreSQLDbContext(string connectionString)
        {
            ConnectionString = connectionString;
        }
    }
}

