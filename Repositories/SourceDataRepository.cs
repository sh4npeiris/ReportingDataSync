using Microsoft.Data.SqlClient;
using ReportingDataSync.Interfaces;

namespace ReportingDataSync.Repositories
{
    public class SourceDataRepository : ISourceDataRepository
    {
        private readonly SqlConnection _productionConnection;

        public SourceDataRepository(SqlConnection productionConnection)
        {
            _productionConnection = productionConnection;
        }

        public async Task<SqlDataReader> ExecuteSourceQueryAsync(string query, DateTime? lastRunDate)
        {
            var cmd = new SqlCommand(query, _productionConnection);
            if (query.Contains("@lastRunDate") && lastRunDate.HasValue)
            {
                cmd.Parameters.AddWithValue("@lastRunDate", lastRunDate.Value);
            }
            return await cmd.ExecuteReaderAsync();
        }

        public async Task<DateTime?> GetMaxIncrementalValueAsync(string query, string incrementalColumn, DateTime lastRunDate)
        {
            // Preserve original formatting
            string sanitizedQuery = query
                .Replace("\r", " ")
                .Replace("\n", " ")
                .Replace("  ", " ")
                .Trim();

            // Handle aliased columns (e.g., "T.DateCreated")
            string qualifiedColumn = incrementalColumn.Contains('.')
                ? incrementalColumn
                : $"[{incrementalColumn}]";

            var scalarSql = $@"
                SELECT MAX({qualifiedColumn})
                FROM ({sanitizedQuery}) AS T
            ";

            using var cmd = new SqlCommand(scalarSql, _productionConnection);
            cmd.Parameters.AddWithValue("@lastRunDate", lastRunDate);
            var result = await cmd.ExecuteScalarAsync();

            return result as DateTime?;
        }
    }
}
