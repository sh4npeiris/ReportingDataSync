namespace ReportingDataSync.Models.Configuration
{
    public class DatabaseSettings
    {
        public string ProductionServer { get; set; }
        public string ProductionDatabase { get; set; }
        public string LocalReportingDatabase { get; set; }
        public int ConnectionTimeout { get; set; }
    }
}
