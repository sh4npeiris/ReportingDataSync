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

        private async Task ProcessTableAsync(TableConfiguration config)
        {
            try
            {
                bool isIncremental = !string.IsNullOrEmpty(config.IncrementalColumn);
                DateTime? lastRunDate = null;
                DateTime? extractedMaxDate = null;

                _logger.LogInformation("Load mode: {Mode}",
                    isIncremental ? "Incremental" : "Full");

                if (isIncremental)
                {
                    // 1) fetch the previous watermark (or school‐year start)
                    lastRunDate = await _targetRepository.GetLastRunDateAsync(config.TargetTable);
                    _logger.LogInformation("Last run date: {LastRunDate}", lastRunDate);

                    // 2) call GetMaxIncrementalValueAsync to get MAX(dateCol) > lastRunDate
                    extractedMaxDate = await _sourceRepository.GetMaxIncrementalValueAsync(
                        config.SourceQuery,
                        config.IncrementalColumn,
                        lastRunDate.Value
                    );

                    if (!extractedMaxDate.HasValue)
                    {
                        // No new rows since lastRunDate
                        _logger.LogInformation(
                            "No new rows found for {TargetTable} since {LastRunDate}. Skipping bulk load.",
                            config.TargetTable, lastRunDate
                        );
                        return; // exit early, no need to open a reader or copy anything
                    }

                    if (extractedMaxDate.Value <= lastRunDate.Value)
                    {
                        // This can happen if “MAX” is exactly equal (unlikely), but guard anyway
                        _logger.LogInformation(
                            "Max date ({MaxDate}) is not greater than last run date ({LastRunDate}) for {TargetTable}. Skipping.",
                            extractedMaxDate, lastRunDate, config.TargetTable
                        );
                        return;
                    }
                }
                else
                {
                    // Full‐load: truncate first
                    _logger.LogWarning("Truncating table before full load");
                    await _targetRepository.TruncateTableAsync(config.TargetTable);
                }

                // 3) At this point:
                //    - Full load: open reader for entire query (no @lastRunDate param)
                //    - Incremental: open reader for rows WHERE IncrementalColumn > lastRunDate
                using (var reader = await _sourceRepository.ExecuteSourceQueryAsync(config.SourceQuery, lastRunDate))
                {
                    _logger.LogInformation("Starting bulk copy for {TargetTable})", config.TargetTable);

                    int rowsCopied = await _targetRepository.CopyIntoReportingTableAsync(reader, config.TargetTable);

                    _logger.LogInformation("Copied {RowCount} rows to {TargetTable}", rowsCopied, config.TargetTable);
                }

                // 4) If this was an incremental run, update the watermark to the new max
                if (isIncremental && extractedMaxDate.HasValue)
                {
                    await _targetRepository.UpdateLastRunDateAsync(
                        config.TargetTable,
                        extractedMaxDate.Value
                    );
                    _logger.LogInformation(
                        "Watermark for {TargetTable} updated to {NewWatermark}",
                        config.TargetTable, extractedMaxDate.Value
                    );
                }

            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error processing table: {TableName}", config.TargetTable);
            }
        }
    }
}
