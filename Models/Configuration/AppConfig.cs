namespace ReportingDataSync.Models.Configuration
{
    public class AppConfig
    {
        public AzureAdSettings AzureAd { get; set; }
        public DatabaseSettings Database { get; set; }
        public EtlSettings Etl { get; set; }

        public AppConfig()
        {
            AzureAd = new AzureAdSettings();
            Database = new DatabaseSettings();
            Etl = new EtlSettings();
        }
    }
}