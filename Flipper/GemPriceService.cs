using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Coflnet.Sky.Core;
using Coflnet.Sky.Flipper;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;

namespace Coflnet.Sky.Commands.Shared
{
    /// <summary>
    /// This services is managed as part of SkyBackendForFrontend
    /// </summary>
    public class GemPriceService : BackgroundService
    {
        public ConcurrentDictionary<string, int> Prices = new();
        public ConcurrentDictionary<(short, long), string> GemNames = new();
        private RestSharp.RestClient commandsClient;
        private IServiceScopeFactory scopeFactory;
        private ILogger<GemPriceService> logger;
        private IConfiguration configuration;

        public GemPriceService(IServiceScopeFactory scopeFactory, ILogger<GemPriceService> logger, IConfiguration configuration)
        {
            this.commandsClient = new RestSharp.RestClient(configuration["API_BASE_URL"]);
            this.scopeFactory = scopeFactory;
            this.logger = logger;
            this.configuration = configuration;
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            if(configuration["DBCONNECTION"] == null)
            {
                logger.LogWarning("No DBCONNECTION found in configuration, aborting background task. This is okay if this service doesn't need gemstone prices"); 
                return;
            }
            var rarities = new string[] { "PERFECT", "FLAWLESS" };
            var types = new string[] { "RUBY", "JASPER", "JADE", "TOPAZ", "AMETHYST", "AMBER", "SAPPHIRE", "OPAL" };
            await LoadNameLookups(rarities, types);

            while (!stoppingToken.IsCancellationRequested)
            {
                foreach (var perfection in rarities)
                {
                    foreach (var type in types)
                    {
                        var itemId = $"{perfection}_{type}_GEM";
                        var route = $"/api/item/price/{itemId}/current";
                        try
                        {
                            var result = await commandsClient.ExecuteAsync(new RestSharp.RestRequest(route));
                            if (result.StatusCode != System.Net.HttpStatusCode.OK)
                                throw new Exception("Response has the status " + result.StatusCode);
                            var profit = JsonConvert.DeserializeObject<CurrentPrice>(result.Content).sell;
                            if (perfection == "PERFECT")
                                profit -= 500_000;
                            else
                                profit -= 100_000;
                            Prices[itemId] = (int)profit;
                        }
                        catch (Exception e)
                        {
                            dev.Logger.Instance.Error(e, "retrieving gem price at " + route);
                        }
                    }
                }
                logger.LogInformation("Loaded gemstone prices");
                await Task.Delay(TimeSpan.FromMinutes(10),stoppingToken);
            }
        }

        private async Task LoadNameLookups(string[] rarities, string[] types)
        {
            using var scope = scopeFactory.CreateScope();
            using var db = scope.ServiceProvider.GetRequiredService<HypixelContext>(); 
            var StringKeys = new List<string>();
            foreach (var item in types)
            {
                for (int i = 0; i < 4; i++)
                {
                    StringKeys.Add($"{item}_{i}");
                }
            }
            var Keys = await db.NBTKeys.Where(n => StringKeys.Contains(n.Slug)).ToListAsync();
            foreach (var key in Keys)
            {
                foreach (var rarity in rarities)
                {
                    var value = await db.NBTValues.Where(n => n.KeyId == key.Id && n.Value == rarity).FirstOrDefaultAsync();
                    if (value == default)
                        continue;
                    GemNames[(key.Id, value.Id)] = $"{rarity}_{key.Slug.Split("_").First()}_GEM";
                }
            }
        }

        /// <summary>
        /// Gets a number that represents the amount of coins the gems on a given auction are worth
        /// </summary>
        /// <param name="auction"></param>
        /// <returns></returns>
        public async Task<int> GetGemstoneWorth(SaveAuction auction)
        {
            if(auction == null)
                return 0;
            if (auction.NbtData == null)
            {
                var lookup = auction.NBTLookup;
                // from db
                return GetGemWrthFromLookup(lookup);
            }
            var gems = auction.FlatenedNBT.Where(n => n.Value == "PERFECT" || n.Value == "FLAWLESS");
            var additionalWorth = 0;
            if (gems.Any())
            {
                foreach (var g in gems)
                {
                    var type = g.Key.Split("_").First();
                    if (type == "COMBAT" || type == "DEFENSIVE" || type == "UNIVERSAL")
                        type = auction.FlatenedNBT[g.Key + "_gem"];
                    if (Prices.TryGetValue($"{g.Value}_{type}_GEM", out int value))
                        additionalWorth += value;
                }
            }

            return additionalWorth;
        }

        public int GetGemWrthFromLookup(List<NBTLookup> lookup)
        {
            return lookup?.Sum(l =>
            {
                if (GemNames.TryGetValue((l.KeyId, l.Value), out string key))
                    if (Prices.TryGetValue(key, out int value))
                    {
                        return value;
                    }
                    else 
                        logger.LogDebug("Price for not found " + key);
                
                return 0;
            }) ?? 0;
        }
    }
}