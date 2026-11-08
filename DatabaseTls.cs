using System;
using System.Threading.Tasks;
using Coflnet.Core.Security;
using Coflnet.Sky.Core;
using Microsoft.Extensions.Configuration;
using MySqlConnector;

namespace Coflnet.Sky.Flipper;

internal sealed class DatabaseTls
{
    private readonly PemTlsOptions credentials;

    internal DatabaseTls(IConfiguration configuration)
    {
        var section = configuration.GetSection("DB:TLS");
        if (!section.Exists())
            return;
        foreach (var key in new[] { "CA_CERT_PATH", "CERT_PATH", "KEY_PATH" })
            if (string.IsNullOrWhiteSpace(section[key]))
                throw new InvalidOperationException($"DB:TLS:{key} must be configured.");
        credentials = PemTlsOptions.FromConfiguration(section);
    }

    internal void Install() => HypixelContext.ConnectionFactory = credentials == null ? null : CreateConnection;

    internal MySqlConnection CreateConnection(string connectionString)
    {
        if (credentials == null)
            return new MySqlConnection(connectionString);
        var options = new MySqlConnectionStringBuilder(connectionString);
        options.ConnectionTimeout = options.ConnectionTimeout == 0 ? 10 : Math.Min(options.ConnectionTimeout, 10);
        options.ConnectionLifeTime = options.ConnectionLifeTime == 0 ? 300 : Math.Min(options.ConnectionLifeTime, 300);
        // Required + unconditional strict callback also rejects password-bound TLS validation failures.
        options.SslMode = MySqlSslMode.Required;
        options.SslCa = options.SslCert = options.SslKey = "";
        options.CertificateFile = options.CertificatePassword = options.CertificateThumbprint = "";
        options.CertificateStoreLocation = MySqlCertificateStoreLocation.None;
        var connection = new MySqlConnection(options.ConnectionString);
        connection.RemoteCertificateValidationCallback = credentials.ValidateServerCertificate;
        connection.ProvideClientCertificatesCallback = certificates =>
        {
            credentials.RegisterClientIntermediates();
            certificates.Add(credentials.GetCertificate());
            return ValueTask.CompletedTask;
        };
        return connection;
    }
}
