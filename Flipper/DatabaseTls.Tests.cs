using System;
using System.Collections.Generic;
using System.IO;
using Coflnet.Sky.Core;
using Microsoft.Extensions.Configuration;
using MySqlConnector;
using NUnit.Framework;

namespace Coflnet.Sky.Flipper;

[NonParallelizable]
public class DatabaseTlsTests
{
    private const string Legacy = "Server=legacy;User ID=svc;Password=synthetic-only;SslMode=None;ConnectionTimeout=90";

    [TearDown]
    public void ResetFactory() => HypixelContext.ConnectionFactory = null;

    [Test]
    public void WithoutTlsKeysFactoryIsNotInstalledAndConnectionIsUnchanged()
    {
        var tls = new DatabaseTls(Configuration(new()));
        tls.Install();
        Assert.That(HypixelContext.ConnectionFactory, Is.Null);
        using var connection = tls.CreateConnection(Legacy);
        var options = new MySqlConnectionStringBuilder(connection.ConnectionString);
        Assert.Multiple(() =>
        {
            Assert.That(options.SslMode, Is.EqualTo(MySqlSslMode.None));
            Assert.That(options.ConnectionTimeout, Is.EqualTo(90));
            Assert.That(options.Password, Is.EqualTo("synthetic-only"));
            Assert.That(connection.RemoteCertificateValidationCallback, Is.Null);
            Assert.That(connection.ProvideClientCertificatesCallback, Is.Null);
        });
    }

    [Test]
    public void WithTlsKeysFactoryIsInstalledAndTlsOptionsAreApplied()
    {
        var tls = new DatabaseTls(Configuration(CompleteKeys()));
        tls.Install();
        Assert.That(HypixelContext.ConnectionFactory, Is.Not.Null);
        using var connection = HypixelContext.ConnectionFactory(Legacy + ";SslCa=old.pem");
        var options = new MySqlConnectionStringBuilder(connection.ConnectionString);
        Assert.Multiple(() =>
        {
            Assert.That(options.SslMode, Is.EqualTo(MySqlSslMode.Required));
            Assert.That(options.Password, Is.EqualTo("synthetic-only"));
            Assert.That(options.ConnectionTimeout, Is.EqualTo(10));
            Assert.That(options.ConnectionLifeTime, Is.EqualTo(300));
            Assert.That(options.SslCa, Is.Empty);
            Assert.That(connection.RemoteCertificateValidationCallback, Is.Not.Null);
            Assert.That(connection.ProvideClientCertificatesCallback, Is.Not.Null);
        });
    }

    [Test]
    public void MissingCertificateFilesFailClosedOnlyWhenTheHandshakeAsksForThem()
    {
        var tls = new DatabaseTls(Configuration(CompleteKeys()));
        using var connection = tls.CreateConnection(Legacy);
        Assert.That(connection.ProvideClientCertificatesCallback, Is.Not.Null);
        Assert.Throws<FileNotFoundException>(() => connection.ProvideClientCertificatesCallback(new()));
    }

    [TestCase("CA_CERT_PATH")]
    [TestCase("CERT_PATH")]
    [TestCase("KEY_PATH")]
    public void PartialTlsConfigurationFailsClosed(string missing)
    {
        var values = CompleteKeys();
        values.Remove("DB:TLS:" + missing);
        Assert.Throws<InvalidOperationException>(() => new DatabaseTls(Configuration(values)));
    }

    private static Dictionary<string, string> CompleteKeys() => new()
    {
        ["DB:TLS:CA_CERT_PATH"] = "missing-roots.pem",
        ["DB:TLS:CERT_PATH"] = "missing-client.pem",
        ["DB:TLS:KEY_PATH"] = "missing-client.key",
    };

    private static IConfiguration Configuration(Dictionary<string, string> values)
        => new ConfigurationBuilder().AddInMemoryCollection(values).Build();
}
