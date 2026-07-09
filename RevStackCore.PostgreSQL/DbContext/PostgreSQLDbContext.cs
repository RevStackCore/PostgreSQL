using System;
namespace RevStackCore.PostgreSQL.DbContext
{
    public class PostgreSQLDbContext
    {
        public string ConnectionString { get; }
        public string Tenant { get; }
        public PostgreSQLDbContext(string connectionString)
        {
            ConnectionString = connectionString;
        }

        public PostgreSQLDbContext(string tenant, string connectionString)
        {
            ConnectionString = connectionString;
            Tenant = tenant;
        }
    }
}

