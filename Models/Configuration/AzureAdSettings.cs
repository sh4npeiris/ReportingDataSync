namespace ReportingDataSync.Models.Configuration
{
    public class AzureAdSettings
    {
        public string TenantId { get; set; }
        public string ClientId { get; set; }
        public string Authority { get; set; }
        public string RedirectUri { get; set; }

        public AzureAdSettings()
        {
            TenantId = string.Empty;
            ClientId = string.Empty;
            Authority = string.Empty;
            RedirectUri = string.Empty;
        }
    }
}