using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Coflnet.Sky.Items.Client.Api;
using System.Linq;

namespace Coflnet.Sky.Flipper
{
    public class FlipperService : BackgroundService
    {
        IItemsApi itemsApi;
        public FlipperService(IServiceScopeFactory factory, IItemsApi itemsApi)
        {
            Flipper.FlipperEngine.Instance.serviceFactory = factory;
            this.itemsApi = itemsApi;
        }
        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            var allIds = await itemsApi.ItemsIdsGetAsync();
            var tags = new string[] { "MINOS_RELIC", "DWARF_TURTLE_SHELMET", "QUICK_CLAW", "PET_ITEM_TIER_BOOST" };
            Flipper.FlipperEngine.Instance.ValuablePetItemIds = allIds.Where(a => tags.Contains(a.Key)).Select(a => (long)a.Value).ToHashSet();
            await Flipper.FlipperEngine.Instance.ProcessPotentialFlipps(stoppingToken);
        }
    }
}
