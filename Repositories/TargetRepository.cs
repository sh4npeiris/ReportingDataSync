using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using ReportingDataSync.Interfaces;
using ReportingDataSync.Models.Configuration;
using System.Text;

namespace ReportingDataSync.Repositories
{
    public class TargetRepository : ITargetRepository
    {
        private readonly SqlConnection _connection;
        private readonly EtlSettings _settings;
        private readonly ILogger<TargetRepository> _logger;
        private string FullControlTableName => $"[{_settings.SchemaName}].[{_settings.ControlTableName}]";

        public TargetRepository(SqlConnection connection, EtlSettings settings, ILogger<TargetRepository> logger)
        {
            _connection = connection;
            _settings = settings;
            _logger = logger;
        }

        public async Task InitializeSchemaAsync()
        {
            var schemaQuery = $@"
            IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = '{_settings.SchemaName}')
            EXEC('CREATE SCHEMA {_settings.SchemaName}')";

            var controlTableQuery = $@"
            IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = '{_settings.ControlTableName}')
            CREATE TABLE {FullControlTableName} (
                TableName VARCHAR(255) NOT NULL,
                LastRunDate DATETIME2(6)
            )";

            await ExecuteNonQueryAsync(schemaQuery);
            await ExecuteNonQueryAsync(controlTableQuery);
        }

        private async Task ExecuteNonQueryAsync(string query)
        {
            using var cmd = new SqlCommand(query, _connection);
            await cmd.ExecuteNonQueryAsync();
        }

        public async Task<DateTime> GetLastRunDateAsync(string tableName)
        {
            var query = $@"
            SELECT COALESCE(
                (SELECT LastRunDate FROM {FullControlTableName} WHERE TableName = @tableName),
                @schoolYearStart)";

            using var cmd = new SqlCommand(query, _connection);
            cmd.Parameters.AddWithValue("@tableName", tableName);
            cmd.Parameters.AddWithValue("@schoolYearStart", GetSchoolYearStart());
            return (DateTime)(await cmd.ExecuteScalarAsync() ?? GetSchoolYearStart());
        }

        public async Task UpdateLastRunDateAsync(string tableName, DateTime newWatermark)
        {
            var query = $@"
                UPDATE {FullControlTableName}
                SET LastRunDate = @newWatermark
                WHERE TableName = @tableName;
        
                IF @@ROWCOUNT = 0
                BEGIN
                    INSERT INTO {FullControlTableName} (TableName, LastRunDate)
                    VALUES (@tableName, @newWatermark)
                END";

            using var cmd = new SqlCommand(query, _connection);
            cmd.Parameters.AddWithValue("@tableName", tableName);
            cmd.Parameters.AddWithValue("@newWatermark", newWatermark);
            await cmd.ExecuteNonQueryAsync();
        }

        private DateTime GetSchoolYearStart()
        {
            if (_settings.ForceSchoolYearStart.HasValue)
                return _settings.ForceSchoolYearStart.Value;

            var now = DateTime.UtcNow;
            int year = now.Month >= _settings.DefaultSchoolYearStartMonth ? now.Year : now.Year - 1;
            return new DateTime(year, _settings.DefaultSchoolYearStartMonth, 1);
        }

        public async Task<int> CopyIntoTableAsync(SqlDataReader dataReader, string targetTable)
        {
            using var tx = _connection.BeginTransaction();
            using var bulkCopy = new SqlBulkCopy(_connection, SqlBulkCopyOptions.CheckConstraints, tx)
            {
                DestinationTableName = targetTable,
                BatchSize = 10000,
                EnableStreaming = true
            };

            await bulkCopy.WriteToServerAsync(dataReader);
            tx.Commit();

            return bulkCopy.RowsCopied;
        }

        public async Task TruncateTableAsync(string tableName)
        {
            var query = $"TRUNCATE TABLE {tableName}";
            await ExecuteNonQueryAsync(query);
        }

        private async Task<List<string>> GetTableColumnsAsync(string tableName)
        {
            var columns = new List<string>();
            var parts = tableName.Replace("[", "").Replace("]", "").Split('.');
            var schema = parts[0];
            var table = parts[1];

            var query = @"
                SELECT COLUMN_NAME
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = @schema AND TABLE_NAME = @table
                ORDER BY ORDINAL_POSITION";

            using var cmd = new SqlCommand(query, _connection);
            cmd.Parameters.AddWithValue("@schema", schema);
            cmd.Parameters.AddWithValue("@table", table);

            using var reader = await cmd.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                columns.Add(reader.GetString(0));
            }
            return columns;
        }

        public async Task MergeDataAsync(string stagingTable, string targetTable, List<string> keyColumns)
        {
            var columns = await GetTableColumnsAsync(targetTable);
            var columnList = string.Join(", ", columns.Select(c => $"[{c}]"));
            var keyJoin = string.Join(" AND ", keyColumns.Select(k => $"target.[{k}] = source.[{k}]"));

            var updateSetters = new StringBuilder();
            foreach (var col in columns)
            {
                if (!keyColumns.Contains(col, StringComparer.OrdinalIgnoreCase))
                {
                    if (updateSetters.Length > 0) updateSetters.Append(", ");
                    updateSetters.Append($"target.[{col}] = source.[{col}]");
                }
            }

            var mergeQuery = new StringBuilder();
            mergeQuery.AppendLine($"MERGE {targetTable} AS target");
            mergeQuery.AppendLine($"USING {stagingTable} AS source");
            mergeQuery.AppendLine($"ON ({keyJoin})");

            // Only add the UPDATE clause if there are non-key columns to update
            if (updateSetters.Length > 0)
            {
                mergeQuery.AppendLine($"WHEN MATCHED THEN");
                mergeQuery.AppendLine($"    UPDATE SET {updateSetters}");
            }

            mergeQuery.AppendLine($"WHEN NOT MATCHED BY TARGET THEN");
            mergeQuery.AppendLine($"    INSERT ({columnList})");
            mergeQuery.AppendLine($"    VALUES ({string.Join(", ", columns.Select(c => $"source.[{c}]"))});");

            await ExecuteNonQueryAsync(mergeQuery.ToString());
        }
    }
}
