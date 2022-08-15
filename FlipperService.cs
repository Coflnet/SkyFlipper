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
        private FlipperEngine flipperEngine;
        public FlipperService(IItemsApi itemsApi, FlipperEngine flipperEngine)
        {
            this.itemsApi = itemsApi;
            this.flipperEngine = flipperEngine;
        }
        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            var allIds = await itemsApi.ItemsIdsGetAsync();
            if(allIds == null)
                throw new System.Exception("Could not load item ids from SkyItemService. Make sure it is reachable.");
            var tags = new string[] { "MINOS_RELIC", "DWARF_TURTLE_SHELMET", "QUICK_CLAW", "PET_ITEM_QUICK_CLAW", "PET_ITEM_TIER_BOOST" };
            flipperEngine.ValuablePetItemIds = allIds.Where(a => tags.Contains(a.Key)).Select(a => (long)a.Value).ToHashSet();
            await flipperEngine.ProcessPotentialFlipps(stoppingToken);
        }
    }
}
