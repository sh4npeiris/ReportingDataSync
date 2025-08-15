using Microsoft.Extensions.Logging;
using ReportingDataSync.Interfaces;
using ReportingDataSync.Models;

namespace ReportingDataSync.Services
{
    public class EtlService
    {
        private readonly ITargetRepository _targetRepository;
        private readonly ISourceDataRepository _sourceRepository;
        private readonly ILogger<EtlService> _logger;

        public EtlService(
            ITargetRepository targetRepository,
            ISourceDataRepository sourceRepository,
            ILogger<EtlService> logger)
        {
            _targetRepository = targetRepository;
            _sourceRepository = sourceRepository;
            _logger = logger;
        }

        public async Task RunEtlProcessAsync(IEnumerable<TableConfiguration> configurations)
        {
            _logger.LogInformation("Starting ETL for {TableCount} tables", configurations.Count());
            try
            {
                await _targetRepository.InitializeSchemaAsync();

                foreach (var config in configurations)
                {
                    _logger.LogInformation("Processing table: {TargetTable}", config.TargetTable);
                    await ProcessTableAsync(config);
                    _logger.LogInformation("Completed table: {TargetTable}", config.TargetTable);
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "ETL process failed");
                throw;
            }
        }

        private string GetStagingTableName(string targetTable)
        {
            var cleanName = targetTable.Replace("[", "").Replace("]", "").Replace('.', '_');
            return $"[ETL].[{cleanName}_Staging]";
        }

        private async Task ProcessTableAsync(TableConfiguration config)
        {
            try
            {
                if (config.IsFullLoad)
                {
                    await ProcessFullLoadAsync(config);
                }
                else
                {
                    await ProcessIncrementalLoadAsync(config);
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error processing table: {TableName}", config.TargetTable);
                throw; // Rethrow to stop the entire process on a single table's failure
            }
        }

        private async Task ProcessFullLoadAsync(TableConfiguration config)
        {
            _logger.LogInformation("Load mode: Full");

            _logger.LogWarning("Truncating target table before full load: {TargetTable}", config.TargetTable);
            await _targetRepository.TruncateTableAsync(config.TargetTable);

            using var reader = await _sourceRepository.ExecuteSourceQueryAsync(config.SourceQuery, null);

            _logger.LogInformation("Starting bulk copy directly to {TargetTable}", config.TargetTable);
            int rowsCopied = await _targetRepository.CopyIntoTableAsync(reader, config.TargetTable);
            _logger.LogInformation("Copied {RowCount} rows to {TargetTable}", rowsCopied, config.TargetTable);
        }

        private async Task ProcessIncrementalLoadAsync(TableConfiguration config)
        {
            _logger.LogInformation("Load mode: Incremental");

            var lastRunDate = await _targetRepository.GetLastRunDateAsync(config.TargetTable);
            _logger.LogInformation("Last run date: {LastRunDate}", lastRunDate);

            var extractedMaxDate = await _sourceRepository.GetMaxIncrementalValueAsync(
                config.SourceQuery, config.IncrementalColumn, lastRunDate);

            if (!extractedMaxDate.HasValue || extractedMaxDate.Value <= lastRunDate)
            {
                _logger.LogInformation(
                    "No new rows found for {TargetTable} since {LastRunDate}. Skipping.",
                    config.TargetTable, lastRunDate);
                return;
            }

            var stagingTable = GetStagingTableName(config.TargetTable);
            await _targetRepository.TruncateTableAsync(stagingTable);

            using (var reader = await _sourceRepository.ExecuteSourceQueryAsync(config.SourceQuery, lastRunDate))
            {
                _logger.LogInformation("Starting bulk copy to staging table {StagingTable}", stagingTable);
                int rowsCopied = await _targetRepository.CopyIntoTableAsync(reader, stagingTable);
                _logger.LogInformation("Copied {RowCount} rows to {StagingTable}", rowsCopied, stagingTable);

                if (rowsCopied > 0)
                {
                    _logger.LogInformation("Merging data from {StagingTable} to {TargetTable}", stagingTable, config.TargetTable);
                    await _targetRepository.MergeDataAsync(stagingTable, config.TargetTable, config.PrimaryKeyColumns);
                }
            }

            await _targetRepository.UpdateLastRunDateAsync(config.TargetTable, extractedMaxDate.Value);
            _logger.LogInformation("Watermark for {TargetTable} updated to {NewWatermark}", config.TargetTable, extractedMaxDate.Value);
        }
    }
}