using Microsoft.Data.SqlClient;
using ReportingDataSync.Interfaces;
using System.Text.RegularExpressions;

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
            // This regex finds the "SELECT ... FROM" part of the query, ignoring case.
            var selectPattern = new Regex(@"SELECT\s+.*?\s+FROM", RegexOptions.IgnoreCase | RegexOptions.Singleline);

            // We create a new query for getting the MAX value, replacing the original SELECT list.
            // This ensures all table aliases (like 'aa') are preserved.
            var scalarSql = selectPattern.Replace(query, $"SELECT MAX({incrementalColumn}) FROM", 1);

            using var cmd = new SqlCommand(scalarSql, _productionConnection);
            cmd.Parameters.AddWithValue("@lastRunDate", lastRunDate);
            var result = await cmd.ExecuteScalarAsync();

            return result as DateTime?;
        }
    }
}
