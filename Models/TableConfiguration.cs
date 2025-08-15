namespace ReportingDataSync.Models
{
    public class TableConfiguration
    {
        public string SourceQuery { get; set; }
        public string TargetTable { get; set; }
        public string IncrementalColumn { get; set; }
        public List<string> PrimaryKeyColumns { get; set; }
        public bool IsFullLoad => string.IsNullOrEmpty(IncrementalColumn);

        public TableConfiguration()
        {
            SourceQuery = string.Empty;
            TargetTable = string.Empty;
            IncrementalColumn = string.Empty;
            PrimaryKeyColumns = new List<string>();
        }
    }
}