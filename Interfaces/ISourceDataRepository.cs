using Microsoft.Data.SqlClient;

namespace ReportingDataSync.Interfaces
{
    public interface ISourceDataRepository
    {
        Task<SqlDataReader> ExecuteSourceQueryAsync(string query, DateTime? lastRunDate);
        Task<DateTime?> GetMaxIncrementalValueAsync(string query, string incrementalColumn, DateTime lastRunDate);
    }
}
