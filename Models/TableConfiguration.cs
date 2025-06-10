namespace ReportingDataSync.Models
{
    public class TableConfiguration
    {
        public string SourceQuery { get; set; }
        public string TargetTable { get; set; }
        public string IncrementalColumn { get; set; }
        public bool IsFullLoad => string.IsNullOrEmpty(IncrementalColumn);
    }
}
