using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Identity.Client;
using ReportingDataSync.Interfaces;
using ReportingDataSync.Models;
using ReportingDataSync.Models.Configuration;
using ReportingDataSync.Repositories;
using ReportingDataSync.Services;
using YamlDotNet.Serialization;
using NLog;
using NLog.Extensions.Logging;
using System.IO;

public class Program
{
    public static async Task Main(string[] args)
    {
        LogManager.Setup().LoadConfigurationFromFile("NLog.config");

        var config = new ConfigurationBuilder()
            .SetBasePath(Directory.GetCurrentDirectory())
            .AddJsonFile("appsettings.json")
            .Build();

        var appConfig = config.Get<AppConfig>() ??
            throw new InvalidOperationException("Invalid configuration");

        var services = new ServiceCollection()
            .AddLogging(logging => 
            {
                logging.ClearProviders();
                logging.SetMinimumLevel(Microsoft.Extensions.Logging.LogLevel.Information);
                logging.AddNLog();
            })
            .AddSingleton(appConfig)
            .AddSingleton(appConfig.Etl)

            .AddScoped<ProductionDbConnection>(sp =>
                new ProductionDbConnection(CreateProductionDbConnection(sp.GetRequiredService<AppConfig>())))
            .AddScoped<SqlConnection>(sp =>
            {
                var config = sp.GetRequiredService<AppConfig>();
                var localConnection = new SqlConnection(config.Database.LocalReportingDatabase);
                localConnection.Open();
                return localConnection;
            })

            .AddTransient<ISourceDataRepository>(sp =>
                new SourceDataRepository(sp.GetRequiredService<ProductionDbConnection>().Value))
            .AddTransient<ITargetRepository>(sp =>
                new TargetRepository(
                    sp.GetRequiredService<SqlConnection>(),
                    sp.GetRequiredService<EtlSettings>(),
                    sp.GetRequiredService<ILogger<TargetRepository>>()))

            .AddTransient<EtlService>();

        var provider = services.BuildServiceProvider();
        var tableConfigs = LoadTableConfigurations();
        var etlService = provider.GetRequiredService<EtlService>();
        await etlService.RunEtlProcessAsync(tableConfigs);
    }

    private static SqlConnection CreateProductionDbConnection(AppConfig config)
    {
        var builder = new SqlConnectionStringBuilder
        {
            DataSource = config.Database.ProductionServer,
            InitialCatalog = config.Database.ProductionDatabase,
            Encrypt = true,
            TrustServerCertificate = false,
            MultipleActiveResultSets = true
        };

        var connection = new SqlConnection(builder.ConnectionString);
        connection.AccessToken = GetProductionAccessToken(config.AzureAd);
        connection.Open();
        return connection;
    }

    private static string GetProductionAccessToken(AzureAdSettings config)
    {
        var app = PublicClientApplicationBuilder.Create(config.ClientId)
            .WithAuthority($"{config.Authority}{config.TenantId}")
            .WithRedirectUri(config.RedirectUri)
            .Build();

        var result = app.AcquireTokenInteractive(new[] { "https://database.windows.net/.default" })
            .ExecuteAsync().GetAwaiter().GetResult();

        return result.AccessToken;
    }

    private static IEnumerable<TableConfiguration> LoadTableConfigurations()
    {
        var path = Path.Combine("TableConfigurations", "etl-tables.yaml");

        // Use a StreamReader to robustly handle file encoding and BOM
        using var reader = new StreamReader(path, detectEncodingFromByteOrderMarks: true);
        var yamlContent = reader.ReadToEnd();

        var deserializer = new DeserializerBuilder()
            .Build();

        return deserializer.Deserialize<List<TableConfiguration>>(yamlContent);
    }
}