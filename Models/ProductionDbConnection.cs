using Microsoft.Data.SqlClient;

namespace ReportingDataSync.Models
{
    public sealed class ProductionDbConnection
    {
        public SqlConnection Value { get; }
        public ProductionDbConnection(SqlConnection connection) => Value = connection;
    }
}
