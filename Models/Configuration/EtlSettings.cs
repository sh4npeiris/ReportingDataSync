namespace ReportingDataSync.Models.Configuration
{
    public class EtlSettings
    {
        public int DefaultSchoolYearStartMonth { get; set; }
        public string SchemaName { get; set; }
        public string ControlTableName { get; set; }
        public DateTime? ForceSchoolYearStart { get; set; }

        public EtlSettings()
        {
            SchemaName = string.Empty;
            ControlTableName = string.Empty;
        }
    }
}