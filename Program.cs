using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Coflnet.Sky.Flipper
{
    public class Program
    {
        public static void Main(string[] args)
        {

            for (int i = 0; i < 2; i++)
                hypixel.Program.RunIsolatedForever(Flipper.FlipperEngine.Instance.ProcessPotentialFlipps, $"flipper worker {i} got error", 1);
            CreateHostBuilder(args).Build().Run();
        }

        public static IHostBuilder CreateHostBuilder(string[] args) =>
            Host.CreateDefaultBuilder(args)
                .ConfigureWebHostDefaults(webBuilder =>
                {
                    webBuilder.UseStartup<Startup>();
                });
    }
}
