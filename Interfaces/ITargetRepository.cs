using Microsoft.Data.SqlClient;

namespace ReportingDataSync.Interfaces
{
    public interface ITargetRepository
    {
        Task InitializeSchemaAsync();
        Task<DateTime> GetLastRunDateAsync(string tableName);
        Task UpdateLastRunDateAsync(string tableName, DateTime newWatermark);
        Task<int> CopyIntoReportingTableAsync(SqlDataReader dataReader, string targetTable);
        Task TruncateTableAsync(string tableName);
        
    }
}
