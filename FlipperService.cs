using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace Coflnet.Sky.Flipper
{
    public class FlipperService : BackgroundService
    {
        public FlipperService(IServiceScopeFactory factory)
        {
            Flipper.FlipperEngine.Instance.serviceFactory = factory;
        }
        protected override Task ExecuteAsync(CancellationToken stoppingToken)
        {
            var first = Flipper.FlipperEngine.Instance.ProcessPotentialFlipps(stoppingToken);
            var second = Flipper.FlipperEngine.Instance.ProcessPotentialFlipps(stoppingToken);
            return Task.WhenAll(first, second);
        }
    }
}
