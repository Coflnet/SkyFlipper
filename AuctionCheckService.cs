using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Coflnet.Sky.Flipper
{
    public class AuctionCheckService : BackgroundService
    {
        private ILogger<AuctionCheckService> logger;
        private FlipperEngine flipperEngine;
        public AuctionCheckService(ILogger<AuctionCheckService> logger, FlipperEngine flipperEngine)
        {
            this.logger = logger;
            this.flipperEngine = flipperEngine;
        }
        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            while (!stoppingToken.IsCancellationRequested)
                try
                {
                    var start = DateTime.UtcNow;
                    await flipperEngine.QueckActiveAuctionsForFlips(stoppingToken);
                    var toWait = start + TimeSpan.FromMinutes(1) - DateTime.UtcNow;
                    if (toWait > TimeSpan.Zero)
                        await Task.Delay(toWait);
                }
                catch (Exception e)
                {
                    logger.LogError(e, "checking auctions for flips");
                }
        }
    }
}
