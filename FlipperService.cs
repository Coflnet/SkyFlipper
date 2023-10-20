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
        private Kafka.KafkaCreator kafkaCreator;
        public FlipperService(IItemsApi itemsApi, FlipperEngine flipperEngine, Kafka.KafkaCreator kafkaCreator)
        {
            this.itemsApi = itemsApi;
            this.flipperEngine = flipperEngine;
            this.kafkaCreator = kafkaCreator;
        }
        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            var allIds = await itemsApi.ItemsIdsGetAsync();
            if(allIds == null)
                throw new System.Exception("Could not load item ids from SkyItemService. Make sure it is reachable.");
            var tags = new string[]
            {
                "MINOS_RELIC",
                "DWARF_TURTLE_SHELMET",
                "QUICK_CLAW",
                "PET_ITEM_QUICK_CLAW",
                "PET_ITEM_TIER_BOOST",
                "GREEN_BANDANA",
                "PET_ITEM_LUCKY_CLOVER",
                "PET_ITEM_EXP_SHARE"
            };
            flipperEngine.ValuablePetItemIds = allIds.Where(a => tags.Contains(a.Key)).Select(a => (long)a.Value).ToHashSet();
            await kafkaCreator.CreateTopicIfNotExist(FlipperEngine.LowPricedAuctionTopic);
            await kafkaCreator.CreateTopicIfNotExist(FlipperEngine.ProduceTopic);
            await flipperEngine.ProcessPotentialFlipps(stoppingToken);
        }
    }
}
