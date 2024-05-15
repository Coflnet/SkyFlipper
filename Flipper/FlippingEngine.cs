using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Coflnet.Sky.Core;
using MessagePack;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Coflnet.Sky.PlayerName.Client.Api;
using Microsoft.Extensions.Configuration;
using System.Diagnostics;
using Coflnet.Sky.Core.Services;

namespace Coflnet.Sky.Flipper
{
    public class FlipperEngine
    {
        public static readonly string ProduceTopic = SimplerConfig.Config.Instance["TOPICS:FLIP"];
        public static readonly string NewAuctionTopic = SimplerConfig.Config.Instance["TOPICS:NEW_AUCTION"];
        public static readonly string KafkaHost = SimplerConfig.Config.Instance["KAFKA_HOST"];
        public static readonly string LowPricedAuctionTopic = SimplerConfig.Config.Instance["TOPICS:LOW_PRICED"];
        /// <summary>
        /// List of ultimate enchantments
        /// </summary>
        public static ConcurrentDictionary<Enchantment.EnchantmentType, bool> UltimateEnchants = new ConcurrentDictionary<Enchantment.EnchantmentType, bool>();
        public static ConcurrentDictionary<Enchantment.EnchantmentType, byte> RelevantEnchants = new ConcurrentDictionary<Enchantment.EnchantmentType, byte>();

        internal IServiceScopeFactory serviceFactory;
        private Commands.Shared.GemPriceService gemPriceService;
        private IPlayerNameApi playerNameApi;
        private Kafka.KafkaCreator kafkaCreator;
        private IConfiguration config;
        private ActivitySource activitySource;

        Prometheus.Counter foundFlipCount = Prometheus.Metrics
                    .CreateCounter("flips_found", "Number of flips found");
        Prometheus.Counter alreadySold = Prometheus.Metrics
                    .CreateCounter("already_sold_flips", "Flips that were already sold for premium users for some reason");
        Prometheus.Histogram time = Prometheus.Metrics.CreateHistogram("time_to_find_flip", "How long did it take to find a flip", new Prometheus.HistogramConfiguration()
        {
            Buckets = Prometheus.Histogram.LinearBuckets(start: 7, width: 2, count: 10)
        });
        static Prometheus.HistogramConfiguration buckets = new Prometheus.HistogramConfiguration()
        {
            Buckets = Prometheus.Histogram.LinearBuckets(start: 7, width: 1, count: 10)
        };
        static Prometheus.Histogram runtroughTime = Prometheus.Metrics.CreateHistogram("sky_flipper_auction_to_send_flip_seconds", "Seconds from loading the auction to finding the flip. (should be close to 10)",
            buckets);
        static Prometheus.Histogram receiveTime = Prometheus.Metrics.CreateHistogram("sky_flipper_auction_receive_seconds", "Seconds that the flipper received an auction. (should be close to 10)",
            buckets);

        public DateTime LastLiveProbe = DateTime.Now;
        private SemaphoreSlim throttler = new SemaphoreSlim(25);
        private static readonly DateTime UnlockedIntroduction = new DateTime(2021, 9, 4);

        public HashSet<long> ValuablePetItemIds = new();
        private HypixelItemService itemService;

        private static readonly Dictionary<string, short> ShardAttributes = new(){
            {"mana_pool", 1},
            {"breeze", 1},
            {"speed", 2},
            {"life_regeneration", 2}, // especially valuable in combination with mana_pool
            {"fishing_experience", 2},
            {"ignition", 2},
            {"blazing_fortune", 2},
            {"double_hook", 3},
            {"mana_regeneration", 2},
            {"mending", 3},
            {"dominance", 3},
            {"magic_find", 2},
            {"veteran", 1}
            //{"lifeline", 3} to low volume
            // life recovery 3
        };

        static FlipperEngine()
        {
            foreach (var item in Enum.GetValues(typeof(Enchantment.EnchantmentType)).Cast<Enchantment.EnchantmentType>())
            {
                if (item.ToString().StartsWith("ultimate_", true, null))
                {
                    UltimateEnchants.TryAdd(item, true);
                }
            }
            foreach (var item in Coflnet.Sky.Core.Constants.RelevantEnchants)
            {
                RelevantEnchants.AddOrUpdate(item.Type, item.Level, (k, o) => item.Level);
            }
        }

        public FlipperEngine(IServiceScopeFactory factory, Commands.Shared.GemPriceService gemPriceService, IPlayerNameApi playerNameApi, Kafka.KafkaCreator kafkaCreator, IConfiguration config, ActivitySource activitySource, HypixelItemService itemService)
        {
            this.serviceFactory = factory;
            this.gemPriceService = gemPriceService;
            this.playerNameApi = playerNameApi;
            this.kafkaCreator = kafkaCreator;
            this.config = config;
            this.activitySource = activitySource;
            this.itemService = itemService;
        }

        private RestSharp.RestClient apiClient = new RestSharp.RestClient(SimplerConfig.Config.Instance["api_base_url"]);


        public Task ProcessPotentialFlipps()
        {
            return ProcessPotentialFlipps(CancellationToken.None);
        }

        public async Task QueckActiveAuctionsForFlips(CancellationToken stopToken)
        {
            using var lpp = kafkaCreator.BuildProducer<string, LowPricedAuction>();
            using var context = new HypixelContext();
            var max = DateTime.UtcNow.Add(TimeSpan.FromMinutes(2));
            var min = DateTime.UtcNow.Add(TimeSpan.FromMinutes(0.5));
            var taskFactory = new TaskFactory(new LimitedConcurrencyLevelTaskScheduler(1));
            var toCheck = context.Auctions
                .Where(a => a.Id > context.Auctions.Max(a => a.Id) - 4000000 && a.End < max && a.End > min && !a.Bin)
                .Include(a => a.NbtData)
                .Include(a => a.Enchantments)
                .Include(a => a.NBTLookup);

            foreach (var auction in toCheck)
            {
                using var span = activitySource.CreateActivity("AuctionFlip", ActivityKind.Server)?
                                           .SetTag("uuid", auction.Uuid);
                try
                {
                    var res = await NewAuction(auction, lpp, span);
                    if (res != null)
                        Console.WriteLine($"Found bid flip {res.Uuid}");
                }
                catch (System.OutOfMemoryException)
                {
                    Console.WriteLine("Out of memory");
                    // stop program
                    Environment.Exit(69);
                }
                catch (Exception e)
                {
                    Console.WriteLine("testing auction" + e);
                }
            }
            Console.WriteLine($"Checked {toCheck.Count()} auctions for flips");
        }

        public async Task ProcessPotentialFlipps(CancellationToken cancleToken)
        {
            try
            {
                Console.WriteLine("starting worker");
                await itemService.GetItemsAsync();
                var taskFactory = new TaskFactory(new LimitedConcurrencyLevelTaskScheduler(2));
                //using (var c = new ConsumerBuilder<Ignore, SaveAuction>(conf).SetValueDeserializer(SerializerFactory.GetDeserializer<SaveAuction>()).Build())
                using var lpp = kafkaCreator.BuildProducer<string, LowPricedAuction>();
                using var p = kafkaCreator.BuildProducer<string, FlipInstance>();
                await Kafka.KafkaConsumer.Consume<SaveAuction>(config, NewAuctionTopic, auction =>
                    {

                        LastLiveProbe = DateTime.Now;
                        // makes no sense to check old auctions
                        if (auction.Start < DateTime.Now - TimeSpan.FromMinutes(5) || !auction.Bin)
                            return Task.CompletedTask;
                        using var span = activitySource.CreateActivity("AuctionFlip", ActivityKind.Server)
                                           ?.SetTag("uuid", auction.Uuid);
                        try
                        {
                            ProcessPotentialFlips(auction, taskFactory, lpp, p, span);
                        }
                        catch (System.OutOfMemoryException)
                        {
                            Console.WriteLine("Out of memory");
                            // stop program
                            Environment.Exit(69);
                        }
                        catch (Exception e)
                        {
                            Console.WriteLine("testing auction" + e);
                        }
                        return Task.CompletedTask;
                    }, cancleToken, "sky-flipper");
            }
            catch (Exception e)
            {
                dev.Logger.Instance.Error($"Flipper threw an exception {e.Message} {e.StackTrace}");
            }
        }

        private void ProcessPotentialFlips(SaveAuction auction, TaskFactory taskFactory, IProducer<string, LowPricedAuction> lpp, IProducer<string, FlipInstance> p, Activity span)
        {
            _ = taskFactory.StartNew(async () =>
            {
                try
                {
                    await throttler.WaitAsync();
                    await ProcessSingleFlip(p, auction, lpp, span);
                }
                catch (Exception e)
                {
                    dev.Logger.Instance.Error(e, "flip search");
                }
                finally
                {
                    throttler.Release();
                }
            });
        }

        private async Task ProcessSingleFlip(IProducer<string, FlipInstance> p, SaveAuction auction, IProducer<string, LowPricedAuction> lpp, Activity span)
        {
            receiveTime.Observe((DateTime.Now - auction.FindTime).TotalSeconds);

            var startTime = DateTime.Now;
            FlipInstance flip = null;

            flip = await NewAuction(auction, lpp, span);

            if (flip != null)
            {
                var timetofind = (DateTime.Now - flip.Auction.FindTime).TotalSeconds;
                if (flip.Auction.Context != null)
                    flip.Auction.Context["fsend"] = (DateTime.Now - flip.Auction.FindTime).ToString();

                p.Produce(ProduceTopic, new Message<string, FlipInstance> { Value = flip, Key = flip.UId.ToString() }, report =>
                {
                    if (report.TopicPartitionOffset.Offset % 200 == 0)
                        Console.WriteLine($"found flip {report.TopicPartitionOffset.Offset} took {timetofind}");
                });
                runtroughTime.Observe(timetofind);
            }
        }

        private uint _auctionCounter = 0;

        public ConcurrentDictionary<long, List<long>> relevantAuctionIds = new ConcurrentDictionary<long, List<long>>();

        public async System.Threading.Tasks.Task<FlipInstance> NewAuction(SaveAuction auction, IProducer<string, LowPricedAuction> lpp, Activity span)
        {
            // blacklist
            if (auction.ItemName == "null" || auction.Tag == "ATTRIBUTE_SHARD" || auction.Tag.Contains(":"))
                return null;

            var price = (auction.HighestBidAmount == 0 ? auction.StartingBid : (auction.HighestBidAmount * 1.1)) / auction.Count;
            if (price < 3000) // only care about auctions worth more than the fee
                return null;


            if (auction.NBTLookup == null || auction.NBTLookup.Count() == 0)
                auction.NBTLookup = NBT.CreateLookup(auction);

            var trackingContext = new FindTracking() { Span = span };
            var referenceElement = await GetRelevantAuctionsCache(auction, trackingContext);
            var relevantAuctions = referenceElement.references;

            long medianPrice = 0;
            if (relevantAuctions.Count <= 3)
            {
                Console.WriteLine($"Not enough relevant auctions for {referenceElement.Key} {auction.Uuid} ({ExtractRelevantEnchants(auction).Count} {relevantAuctions.Count})");

                // the overall median was deemed to inaccurate
                return null;
                /* var itemId = ItemDetails.Instance.GetItemIdForName(auction.Tag, false);
                 var lookupPrices = await ItemPrices.GetLookupForToday(itemId);
                 if (lookupPrices?.Prices.Count > 0)
                     medianPrice = (long)(lookupPrices?.Prices?.Average(p => p.Avg * 0.8 + p.Min * 0.2) ?? 0);*/
            }
            var binRefcount = relevantAuctions.Count(a => a.Bin);
            if (relevantAuctions.Count > binRefcount * 2)
            {
                Console.WriteLine($"Too many auctions that are not BIN {auction.Uuid} bin count {binRefcount} total references {relevantAuctions.Count})");
                // too many auctions that are not BIN
                return null;
            }
            medianPrice = await GetWeightedMedian(auction, relevantAuctions);

            var hitCountReduction = Math.Pow(1.05, referenceElement.HitCount);
            int additionalWorth = await GetGemstoneWorth(auction);
            var recomendedBuyUnder = (medianPrice * 0.9 + additionalWorth) / hitCountReduction;
            if (recomendedBuyUnder < 1_000_000)
            {
                recomendedBuyUnder *= 0.9;
            }

            if (price > recomendedBuyUnder || recomendedBuyUnder < 100_000) // at least 10% profit
            {
                return null; // not a good flip
            }

            relevantAuctionIds[auction.UId] = relevantAuctions.Select(a => a.UId == 0 ? AuctionService.Instance.GetId(a.Uuid) : a.UId).ToList();
            if (relevantAuctionIds.Count > 10000)
            {
                relevantAuctionIds.Clear();
            }

            var additionalProps = new Dictionary<string, string>(trackingContext.Context);
            if (additionalWorth > 0)
            {
                additionalProps["worth"] = additionalWorth.ToString();
                additionalProps["gem_prices"] = string.Join(",", gemPriceService.Prices.Select(p => p.ToString()));
                Console.WriteLine("added prices " + string.Join(",", gemPriceService.Prices.Select(p => p.ToString())));
            }
            if (auction.Context != null)
                auction.Context["fsend"] = (DateTime.Now - auction.FindTime).ToString();
            additionalProps["refAge"] = ((int)(DateTime.UtcNow - referenceElement.Oldest).TotalDays).ToString();

            var countMultiplier = 1d;
            if (auction.Count > 1)
            {
                countMultiplier = 0.9; // more than one has to be cheaper
            }
            var targetPrice = (int)(medianPrice * auction.Count * countMultiplier / hitCountReduction) + additionalWorth;
            var lowPrices = new LowPricedAuction()
            {
                Auction = auction,
                DailyVolume = (float)(relevantAuctions.Count / (DateTime.Now - referenceElement.Oldest).TotalDays),
                Finder = LowPricedAuction.FinderType.FLIPPER,
                TargetPrice = targetPrice,
                AdditionalProps = additionalProps
            };

            lpp.Produce(LowPricedAuctionTopic, new Message<string, LowPricedAuction>()
            {
                Key = auction.Uuid,
                Value = lowPrices
            });
            await SaveHitOnFlip(referenceElement, auction, recomendedBuyUnder);

            return null; // deactivated old flipper
        }

        public async Task<long> GetWeightedMedian(SaveAuction auction, List<SaveAuction> relevantAuctions)
        {
            var auctions = (await Task.WhenAll(relevantAuctions
                                //.OrderByDescending(a => a.HighestBidAmount)
                                .Select(async a => new { a, value = a.HighestBidAmount / (a.Count == 0 ? 1 : a.Count) / (a.Count == auction.Count ? 1 : 3) - await GetGemstoneWorth(a) })))
                                .ToList();
            var fullTime = auctions.Select(a => a.value).OrderByDescending(a => a)
                                .Skip(relevantAuctions.Count / 2)
                                .FirstOrDefault();
            var shortTerm = auctions.OrderByDescending(a => a.a.End).Take(3).OrderByDescending(a => a.value)
                                .Select(a => a.value)
                                .Skip(1)
                                .FirstOrDefault();
            if (auctions.Count > 10 && relevantAuctions.All(a => a.End > DateTime.Now - TimeSpan.FromHours(20)))
            {
                // very high volume item could drop suddenly, use 25th percentile instead
                fullTime = auctions.Select(a => a.value).OrderBy(a => a)
                                .Skip(relevantAuctions.Count / 4)
                                .FirstOrDefault();
            }

            return Math.Min(fullTime, shortTerm);
        }

        private async Task SaveHitOnFlip(RelevantElement referenceElement, SaveAuction auction, double recomendedBuyUnder)
        {
            var storeLength = TimeSpan.FromHours(2);
            if (referenceElement.HitCount % 3 == 0)
                Console.WriteLine($"hit {referenceElement.Key} {referenceElement.HitCount} times at price {recomendedBuyUnder}");

            var storeTime = referenceElement.QueryTime - DateTime.Now + storeLength;
            if (storeTime < TimeSpan.Zero || referenceElement.HitCount > 10)
                storeTime = TimeSpan.FromSeconds(1);
            referenceElement.HitCount++;
            // is set to fireand forget (will return imediately)
            await CacheService.Instance.SaveInRedis(referenceElement.Key, referenceElement, storeTime);
            if (ShouldReferencesBeReloaded(referenceElement, storeLength, storeTime))
            {
                ReloadReferencesIn10Seconds(referenceElement, auction);
            }
        }

        private static bool ShouldReferencesBeReloaded(RelevantElement referenceElement, TimeSpan storeLength, TimeSpan storeTime)
        {
            return storeTime < storeLength * 0.9 && referenceElement.HitCount > 3;
        }

        private void ReloadReferencesIn10Seconds(RelevantElement referenceElement, SaveAuction auction)
        {
            _ = Task.Run(async () =>
            {
                await Task.Delay(TimeSpan.FromSeconds(10));
                try
                {
                    await GetAndCacheReferenceAuctions(auction, new FindTracking(), referenceElement.Key);
                }
                catch (Exception e)
                {
                    dev.Logger.Instance.Error(e, "refreshing cache for " + referenceElement?.Key);
                }
            }).ConfigureAwait(false);
        }

        private async Task<int> GetGemstoneWorth(SaveAuction auction)
        {
            return await gemPriceService.GetGemstoneWorth(auction);
        }

        private static HashSet<string> ignoredNbt = new HashSet<string>()
                { "uid", "spawnedFor", "bossId", "exp", "uuid", "hpc", "active", "uniqueId", "hideRightClick", "noMove" };
        /// <summary>
        /// Gets relevant items for an auction, checks cache first
        /// </summary>
        /// <param name="auction"></param>
        /// <param name="tracking"></param>
        /// <returns></returns>
        public async Task<RelevantElement> GetRelevantAuctionsCache(SaveAuction auction, FindTracking tracking)
        {
            string key = GetCacheKey(auction);
            tracking.Tag("key", key);
            try
            {
                var fromCache = await CacheService.Instance.GetFromRedis<RelevantElement>(key);
                if (fromCache != default(RelevantElement))
                {
                    fromCache.Key = key;
                    tracking.Tag("cache", "true");
                    return fromCache;
                }
            }
            catch (Exception e)
            {
                dev.Logger.Instance.Error(e, "cache flip");
            }

            return await GetAndCacheReferenceAuctions(auction, tracking, key);
        }

        private static string GetCacheKey(SaveAuction auction)
        {
            var key = $"o{auction.Tag}{auction.ItemName}{auction.Tier}{auction.Count}";
            if (relevantReforges.Contains(auction.Reforge))
                key += auction.Reforge;
            var relevant = ExtractRelevantEnchants(auction);
            if (relevant.Count() == 0)
                key += String.Concat(auction.Enchantments.Select(a => $"{a.Type}{a.Level}"));
            else
                key += String.Concat(relevant.Select(a => $"{a.Type}{a.Level}"));
            key += String.Concat(auction.FlatenedNBT.Where(d => !ignoredNbt.Contains(d.Key)));
            return key;
        }

        private async Task<RelevantElement> GetAndCacheReferenceAuctions(SaveAuction auction, FindTracking tracking, string key)
        {
            using var scope = serviceFactory.CreateScope();
            using var context = scope.ServiceProvider.GetRequiredService<HypixelContext>();

            var referenceAuctions = await GetRelevantAuctions(auction, context, tracking);
            // shifted out of the critical path
            if (referenceAuctions.references.Count > 1)
            {
                if (referenceAuctions.references.Any(a => a.Tag != auction.Tag))
                    throw new CoflnetException("tag_mismatch", $"tag mismatch {referenceAuctions.references.First().Tag} != {auction.Tag}");
                await CacheService.Instance.SaveInRedis<RelevantElement>(key, referenceAuctions, TimeSpan.FromHours(2));
            }
            tracking.Tag("cache", "false");
            referenceAuctions.Key = key;
            return referenceAuctions;
        }

        private async Task<RelevantElement> GetRelevantAuctions(SaveAuction auction, HypixelContext context, FindTracking tracking)
        {
            var itemData = auction.NbtData.Data;
            var clearedName = auction.Reforge != ItemReferences.Reforge.None ? ItemReferences.RemoveReforge(auction.ItemName) : auction.ItemName;
            var itemId = ItemDetails.Instance.GetItemIdForTag(auction.Tag, false);
            var youngest = DateTime.Now;
            List<Enchantment> relevantEnchants = ExtractRelevantEnchants(auction);
            //var matchingCount = relevantEnchants.Count > 3 ? relevantEnchants.Count * 2 / 3 : relevantEnchants.Count;
            var ulti = relevantEnchants.Where(e => UltimateEnchants.ContainsKey(e.Type)).FirstOrDefault();
            var highLvlEnchantList = relevantEnchants.Where(e => !UltimateEnchants.ContainsKey(e.Type)).Select(a => a.Type).ToList();
            var oldest = DateTime.Now - TimeSpan.FromHours(2);

            var baseSelect = context.Auctions
                .Where(a => a.ItemId == itemId)
                .Include(a => a.Bids)
                .Where(a => a.HighestBidAmount > 0);
            IQueryable<SaveAuction> select = GetSelect(auction, baseSelect, clearedName, youngest, ulti, relevantEnchants, oldest, tracking, 30);

            var relevantAuctions = await select
                .ToListAsync();

            if (relevantAuctions.Count < 11)
            {
                // to few auctions in last hour, try a whole day
                oldest = DateTime.Now - TimeSpan.FromDays(1.5);
                relevantAuctions = relevantAuctions.Concat(await GetSelect(auction, baseSelect, clearedName, youngest, ulti, relevantEnchants, oldest, tracking, 90)
                .ToListAsync()).ToList();

                if (relevantAuctions.Count < 50)
                {
                    // to few auctions in a day, query a week
                    youngest = oldest;
                    oldest = DateTime.Now - TimeSpan.FromDays(8);
                    relevantAuctions.AddRange(await GetSelect(auction, baseSelect, clearedName, youngest, ulti, relevantEnchants, oldest, tracking, 120)
                    .ToListAsync());
                    if (relevantAuctions.Count < 10)
                    {
                        youngest = DateTime.Now;
                        clearedName = clearedName.Replace("✪", "").Trim();
                        relevantAuctions = await GetSelect(auction, baseSelect, clearedName, youngest, ulti, relevantEnchants, oldest, tracking, 120, true)
                        .ToListAsync();
                        // query recent ones with no stars to avoid price drops
                        relevantAuctions.AddRange(await GetSelect(auction, baseSelect, clearedName, youngest, ulti, relevantEnchants, DateTime.Now - TimeSpan.FromDays(0.5), tracking, 10, true)
                        .ToListAsync());
                    }
                }
            }
            else if (relevantAuctions.Count >= 30)
                await AddVeryRecentReferencesForHighVolume(auction, baseSelect, tracking, clearedName, youngest, relevantEnchants, ulti, relevantAuctions);
            if(relevantAuctions.All(a=>a.End > DateTime.UtcNow- TimeSpan.FromDays(1)))
            {
                // Add anti market manipulation data
                relevantAuctions.AddRange(
                                await GetSelect(auction, select, clearedName, youngest, ulti, relevantEnchants, DateTime.Now - TimeSpan.FromDays(6), tracking, 40, true)
                                        .ToListAsync());
            }

            /* got replaced with average overall lookup
            if (relevantAuctions.Count < 3 && PotetialFlipps.Count < 100)
            {
                oldest = DateTime.Now - TimeSpan.FromDays(25);
                relevantAuctions = await GetSelect(auction, context, null, itemId, youngest, matchingCount, ulti, ultiList, highLvlEnchantList, oldest)
                        .ToListAsync();
            } */
            // deduplicate both sellers and buyers
            relevantAuctions = ApplyAntiMarketManipulation(relevantAuctions);

            // filter out auctions that are not master crypts sols
            if (!auction.FlatenedNBT.Any(f => f.Key.StartsWith("MASTER_CRYPT")))
                relevantAuctions = relevantAuctions.Where(a => !a.FlatenedNBT.Any(f => f.Key.StartsWith("MASTER_CRYPT"))).ToList();

            return new RelevantElement()
            {
                references = relevantAuctions,
                Oldest = oldest
            };
        }

        /// <summary>
        /// Deduplicates auctions by seller, item id and buyer (when manipulating few accounts are used)
        /// </summary>
        /// <param name="relevantAuctions"></param>
        /// <returns></returns>
        private static List<SaveAuction> ApplyAntiMarketManipulation(List<SaveAuction> relevantAuctions)
        {
            var counter = 1;
            if (relevantAuctions.Count > 1)
                relevantAuctions = relevantAuctions.GroupBy(a => a.SellerId)
                    .Select(a => a.OrderBy(s => s.HighestBidAmount).First())
                    .GroupBy(a => a.Bids.OrderByDescending(b => b.Amount).First().Bidder)
                    .Select(activitySource => activitySource.First())
                    .GroupBy(a => a.FlatenedNBT.TryGetValue("uid", out string uid) ? uid : counter++.ToString())
                    .Select(a => a.First())
                    .ToList();
            if (counter > 2)
            {
                // this is an item with no uid, check that there is no back-forth trading with same users
                var users = relevantAuctions.Select(a =>
                {
                    var buyer = a.Bids.OrderByDescending(b => b.Amount).First().Bidder;
                    var seller = a.AuctioneerId;
                    // order them to have a consistent order
                    return AuctionService.Instance.GetId(buyer) < AuctionService.Instance.GetId(seller) ? (buyer, seller) : (seller, buyer);
                }).GroupBy(a => a).Where(g => g.Count() > 1).ToList();
                if (users.Count > 0)
                {
                    Console.WriteLine($"found back and forth trading on {relevantAuctions.First().Uuid} {users.Count} users from {users.First().Key}");
                    // exclude all of them
                    relevantAuctions = relevantAuctions.Where(a => !users.Any(u => u.Key.Item1 == a.AuctioneerId || u.Key.Item2 == a.AuctioneerId)).ToList();
                }
            }

            foreach (var item in relevantAuctions)
            {
                // after deduplication not needed
                item.Bids = null;
            }

            return relevantAuctions;
        }

        private async Task AddVeryRecentReferencesForHighVolume(SaveAuction auction, IQueryable<SaveAuction> select, FindTracking tracking, string clearedName, DateTime youngest, List<Enchantment> relevantEnchants, Enchantment ulti, List<SaveAuction> relevantAuctions)
        {
            relevantAuctions.AddRange(
                                await GetSelect(auction, select, clearedName, youngest, ulti, relevantEnchants, DateTime.Now - TimeSpan.FromMinutes(15), tracking, 20, true)
                                        .ToListAsync());
        }

        public static List<Enchantment> ExtractRelevantEnchants(SaveAuction auction)
        {
            return auction.Enchantments?.Where(e => (!RelevantEnchants.ContainsKey(e.Type) && e.Level >= 6) || (RelevantEnchants.TryGetValue(e.Type, out byte lvl)) && e.Level >= lvl)
                .ToList();
        }

        private readonly static HashSet<ItemReferences.Reforge> relevantReforges = Constants.RelevantReforges;

        public IQueryable<SaveAuction> GetSelect(
            SaveAuction auction,
            IQueryable<SaveAuction> select,
            string clearedName,
            DateTime youngest,
            Enchantment ulti,
            List<Enchantment> highLvlEnchantList,
            DateTime oldest,
            FindTracking track,
            int limit = 60,
            bool reduced = false)
        {
            if (auction.Tag != "ENCHANTED_BOOK") // the rarity is often used from scamming but doesn't change price
                select = select.Where(a => a.Tier == auction.Tier);

            byte ultiLevel = 127;
            var flatNbt = auction.FlatenedNBT ?? new Dictionary<string, string>();
            Enchantment.EnchantmentType ultiType = Enchantment.EnchantmentType.unknown;
            if (ulti != null)
            {
                ultiLevel = ulti.Level;
                ultiType = ulti.Type;
            }

            select = AddReforgeSelect(auction, reduced, select);

            if ((auction.Count == 64) && !reduced) // try to match stack count as those are usually cheaper
                select = select.Where(s => s.Count == auction.Count);

            if (auction.ItemName != clearedName && clearedName != null)
                select = select.Where(a => EF.Functions.Like(a.ItemName, "%" + clearedName));
            else if (auction.Tag.StartsWith("PET") && auction.ItemName.StartsWith('['))
            {
                try
                {
                    select = AddPetLvlSelect(auction, select);
                }
                catch (Exception e)
                {
                    dev.Logger.Instance.Error(e, "pet lvl add " + auction.ItemName);
                    return null;
                }
            }
            else
            {
                select = select.Where(a => a.ItemName == clearedName);
            }

            if (auction.Tag == "MIDAS_STAFF" || auction.Tag == "MIDAS_SWORD")
            {
                try
                {
                    var keyValue = "winning_bid";
                    select = AddMidasSelect(select, flatNbt, keyValue);
                    oldest -= TimeSpan.FromDays(10);
                }
                catch (Exception e)
                {
                    dev.Logger.Instance.Error(e, "trying filter flip midas item");
                }
            }

            if (auction.Tag.Contains("HOE") || flatNbt.ContainsKey("farming_for_dummies_count"))
                select = AddNBTSelect(select, flatNbt, "farming_for_dummies_count");

            if (auction.Tag == "CAKE_SOUL")
                select = AddNBTSelect(select, flatNbt, "captured_player");

            if (flatNbt.ContainsKey("rarity_upgrades"))
                select = AddNBTSelect(select, flatNbt, "rarity_upgrades");
            if (auction.Tag == "ASPECT_OF_THE_VOID" || auction.Tag == "ASPECT_OF_THE_END")
                select = AddNBTSelect(select, flatNbt, "ethermerge");
            if (auction.Tag == "NECRONS_LADDER")
                select = AddNBTSelect(select, flatNbt, "handles_found");
            if (flatNbt.ContainsKey("edition"))
                select = AddNbtRangeSelect(select, flatNbt, "edition", 100, 10);

            if (flatNbt.ContainsKey("new_years_cake"))
                select = AddNBTSelect(select, flatNbt, "new_years_cake");
            if ((flatNbt.ContainsKey("dungeon_item_level") || (itemService?.GetStarIngredients(auction.Tag, 1).Any() ?? false)) && !reduced)
                select = AddNBTSelect(select, flatNbt, "dungeon_item_level");
            if (flatNbt.ContainsKey("candyUsed"))
                select = AddCandySelect(select, flatNbt, "candyUsed");
            if (flatNbt.ContainsKey("art_of_war_count"))
                select = AddNBTSelect(select, flatNbt, "art_of_war_count");
            if (flatNbt.ContainsKey("MUSIC"))
                select = AddNBTSelect(select, flatNbt, "MUSIC");
            if (flatNbt.ContainsKey("ENCHANT"))
                select = AddNBTSelect(select, flatNbt, "ENCHANT");
            if (flatNbt.ContainsKey("DRAGON"))
                select = AddNBTSelect(select, flatNbt, "DRAGON");
            if (flatNbt.ContainsKey("TIDAL"))
                select = AddNBTSelect(select, flatNbt, "TIDAL");
            if (flatNbt.ContainsKey("ability_scroll"))
                select = AddNBTSelect(select, flatNbt, "ability_scroll");
            if (flatNbt.ContainsKey("party_hat_emoji"))
                select = AddNBTSelect(select, flatNbt, "party_hat_emoji");
            if (auction.Tag.Contains("FINAL_DESTINATION"))
                select = AddNbtRangeSelect(select, flatNbt, "eman_kills", 1000, 10);

            if (flatNbt.Any(n => Constants.AttributeKeys.Contains(n.Key)))
                foreach (var item in Constants.AttributeKeys)
                {
                    select = AddNBTSelect(select, flatNbt, item);
                }

            if (auction.Tag.Contains("_DRILL"))
            {
                select = AddNBTSelect(select, flatNbt, "drill_part_engine");
                select = AddNBTSelect(select, flatNbt, "drill_part_fuel_tank");
                select = AddNBTSelect(select, flatNbt, "drill_part_upgrade_module");
            }

            select = AddPetItemSelect(select, flatNbt, auction.StartingBid);

            if (flatNbt.ContainsKey("skin"))
            {
                var keyId = NBT.Instance.GetKeyId("skin");
                var val = NBT.GetItemIdForSkin(flatNbt["skin"]);
                select = select.Where(a => a.NBTLookup.Where(n => n.KeyId == keyId && n.Value == val).Any());
            }

            if (flatNbt.ContainsKey("color") || (IsArmour(auction)))
            {
                select = AddNBTSelect(select, flatNbt, "color");
                select = AddNBTSelect(select, flatNbt, "dye_item");
            }

            foreach (var item in flatNbt)
            {
                if (item.Key.EndsWith("_kills"))
                {
                    var keyId = NBT.Instance.GetKeyId(item.Key);
                    var val = Int32.Parse(item.Value);
                    var max = val * 1.2;
                    var min = val * 0.8;
                    select = select.Where(a => a.NBTLookup.Where(n => n.KeyId == keyId && n.Value >= min && n.Value < max).Any());
                }
            }

            bool canHaveGemstones = itemService?.GetUnlockableSlots(auction.Tag)?.Any() ?? false;
            if (canHaveGemstones
                || flatNbt.ContainsKey("unlocked_slots"))
            {
                select = AddNBTSelect(select, flatNbt, "unlocked_slots");
                select = select.Where(a => a.ItemCreatedAt > UnlockedIntroduction);
            }

            if (flatNbt.ContainsKey("gemstone_slots")) // I think this is the old gemstone format
                select = AddNBTSelect(select, flatNbt, "gemstone_slots");

            select = AddEnchantmentSubselect(auction, highLvlEnchantList, select, ultiLevel, ultiType, track, reduced);
            if (limit == 0)
                return select;

            return select
                .Where(a => a.End > oldest && a.End < youngest)
                //.OrderByDescending(a=>a.Id)
                .Include(a => a.NbtData)
                .Take(limit)
                .AsSplitQuery();
        }

        private static bool IsArmour(SaveAuction auction)
        {
            return auction.Tag.EndsWith("_CHESTPLATE") || auction.Tag.EndsWith("_BOOTS") || auction.Tag.EndsWith("_HELMET") || auction.Tag.EndsWith("_LEGGINGS");
        }

        private static IQueryable<SaveAuction> AddReforgeSelect(SaveAuction auction, bool reduced, IQueryable<SaveAuction> select)
        {
            // ancient is low volume (hasn't that many references)
            var shouldDropAncient = reduced && auction.Reforge == ItemReferences.Reforge.ancient;
            if (relevantReforges.Contains(auction.Reforge) && !shouldDropAncient)
                select = select.Where(a => a.Reforge == auction.Reforge);
            else
                select = select.Where(a => !relevantReforges.Contains(a.Reforge));
            return select;
        }

        private IQueryable<SaveAuction> AddPetItemSelect(IQueryable<SaveAuction> select, Dictionary<string, string> flatNbt, long startingBid)
        {
            if (flatNbt.ContainsKey("heldItem"))
            {
                var keyId = NBT.Instance.GetKeyId("heldItem");
                var val = ItemDetails.Instance.GetItemIdForTag(flatNbt["heldItem"]);
                // only include boosts if there are still exp to be boosted
                if (ShouldPetItemMatch(flatNbt, startingBid))
                    select = select.Where(a => a.NBTLookup.Where(n => n.KeyId == keyId && n.Value == val).Any());
                else
                    select = select.Where(a => !a.NBTLookup.Where(n => n.KeyId == keyId && ValuablePetItemIds.Contains(n.Value)).Any());
            }
            else if (flatNbt.ContainsKey("candyUsed")) // every pet has candyUsed attribute
            {
                var keyId = NBT.Instance.GetKeyId("heldItem");
                select = select.Where(a => !a.NBTLookup.Where(n => n.KeyId == keyId).Any());
            }

            return select;
        }

        public static bool ShouldPetItemMatch(Dictionary<string, string> flatNbt, long startingBid)
        {
            if (!flatNbt.ContainsKey("heldItem") || !flatNbt.ContainsKey("exp"))
                return false;
            var shouldMatch = flatNbt["heldItem"] switch
            {
                "MINOS_RELIC" => true,
                "QUICK_CLAW" => true,
                "PET_ITEM_QUICK_CLAW" => true,
                "PET_ITEM_TIER_BOOST" => true,
                "PET_ITEM_LUCKY_CLOVER" => true,
                "PET_ITEM_LUCKY_CLOVER_DROP" => true,
                "GREEN_BANDANA" => true,
                "PET_ITEM_COMBAT_SKILL_BOOST_EPIC" => true,
                "PET_ITEM_FISHING_SKILL_BOOST_EPIC" => true,
                "PET_ITEM_FORAGING_SKILL_BOOST_EPIC" => true,
                "ALL_SKILLS_SUPER_BOOST" => true,
                "PET_ITEM_EXP_SHARE" => true,
                _ => false
            };
            if (shouldMatch)
                return shouldMatch;

            return (flatNbt.TryGetValue("exp", out string expString) && double.Parse(expString) < 24_000_000 || !(flatNbt["heldItem"].Contains("SKILL") && flatNbt["heldItem"].Contains("BOOST")))
                && startingBid < 20_000_000; // don't care about low value boosts on expensive items
        }

        private static IQueryable<SaveAuction> AddNBTSelect(IQueryable<SaveAuction> select, Dictionary<string, string> flatNbt, string keyValue)
        {
            var keyId = NBT.Instance.GetKeyId(keyValue);
            if (!flatNbt.ContainsKey(keyValue))
                return select.Where(a => !a.NBTLookup.Where(n => n.KeyId == keyId).Any());

            if (!long.TryParse(flatNbt[keyValue], out long val))
            {
                val = NBT.Instance.GetValueId(keyId, flatNbt[keyValue]);
                return select.Where(a => a.NBTLookup.Where(n => n.KeyId == keyId && n.Value == val).Any());
            }
            return select.Where(a => a.NBTLookup.Where(n => n.KeyId == keyId && n.Value <= val).Any());
        }

        private static IQueryable<SaveAuction> AddCandySelect(IQueryable<SaveAuction> select, Dictionary<string, string> flatNbt, string keyValue)
        {
            var keyId = NBT.Instance.GetKeyId(keyValue);
            long.TryParse(flatNbt[keyValue], out long val);
            if (val > 0)
                return select.Where(a => a.NBTLookup.Where(n => n.KeyId == keyId && n.Value > 0).Any());
            return select.Where(a => a.NBTLookup.Where(n => n.KeyId == keyId && n.Value == 0).Any());
        }

        private static IQueryable<SaveAuction> AddMidasSelect(IQueryable<SaveAuction> select, Dictionary<string, string> flatNbt, string keyValue)
        {
            var maxDiff = 2_000_000;
            return AddNbtRangeSelect(select, flatNbt, keyValue, maxDiff);
        }

        /// <summary>
        /// Adds a range select to get relevant auctions
        /// </summary>
        /// <param name="select">the db select</param>
        /// <param name="flatNbt">flat nbt of the target tiem</param>
        /// <param name="keyValue">The NBTkey to use</param>
        /// <param name="maxDiff">By how much the range is extended in both directions</param>
        /// <param name="percentIncrease">How many percent difference should be added to maxDiff</param>
        /// <returns></returns>
        private static IQueryable<SaveAuction> AddNbtRangeSelect(IQueryable<SaveAuction> select, Dictionary<string, string> flatNbt, string keyValue, long maxDiff, int percentIncrease = 0)
        {
            var keyId = NBT.Instance.GetKeyId(keyValue);
            if (!flatNbt.TryGetValue(keyValue, out var stringValue))
            {
                return select.Where(a => !a.NBTLookup.Where(n => n.KeyId == keyId).Any());
            }
            var val = long.Parse(stringValue);
            maxDiff += val * percentIncrease;
            select = select.Where(a => a.NBTLookup.Where(n => n.KeyId == keyId && n.Value > val - maxDiff && n.Value < val + maxDiff).Any());
            return select;
        }

        private static IQueryable<SaveAuction> AddPetLvlSelect(SaveAuction auction, IQueryable<SaveAuction> select)
        {
            string value = GetPetLevelSelectVal(auction);
            select = select.Where(a => EF.Functions.Like(a.ItemName, value));
            return select;
        }

        public static string GetPetLevelSelectVal(SaveAuction auction)
        {
            var sb = new StringBuilder(auction.ItemName);
            if (sb[6] == ']')
                sb[5] = '_';
            else if (sb[8] == ']')
                sb[7] = '_'; // [Lvl 10_]
            else
                sb[6] = '_';
            var value = sb.ToString();
            return value;
        }

        public static IQueryable<SaveAuction> AddEnchantmentSubselect(SaveAuction auction, List<Enchantment> highLvlEnchantList, IQueryable<SaveAuction> select, byte ultiLevel, Enchantment.EnchantmentType ultiType, FindTracking track, bool reduced = false)
        {
            var relevant = RelevantEnchants.Select(r => r.Key).ToList();
            if (highLvlEnchantList.Count > 0)
            {
                var matchingCount = highLvlEnchantList.Count;
                if (reduced) // reduce to find match
                    highLvlEnchantList = new List<Enchantment>() { Constants.SelectBest(auction.Enchantments) };
                var maxImportantEnchants = highLvlEnchantList.Count() + 1 + (ultiType == Enchantment.EnchantmentType.unknown ? 0 : 1);
                var highLevel = highLvlEnchantList.Select(r => new { r.Type, r.Level }).ToList();
                var highLvlHash = highLvlEnchantList.Select(r => r.Type).ToHashSet();

                var minLvl1 = GetMinLvl(highLvlEnchantList, 1);
                var minLvl2 = GetMinLvl(highLvlEnchantList, 2);
                var minLvl3 = GetMinLvl(highLvlEnchantList, 3);
                var minLvl4 = GetMinLvl(highLvlEnchantList, 4);
                var minLvl5 = GetMinLvl(highLvlEnchantList, 5);
                var minLvl6 = GetMinLvl(highLvlEnchantList, 6);
                var minLvl7 = GetMinLvl(highLvlEnchantList, 7);
                var minLvl8 = GetMinLvl(highLvlEnchantList, 8);
                var minLvl9 = GetMinLvl(highLvlEnchantList, 9);
                var minLvl10 = GetMinLvl(highLvlEnchantList, 10);

                var missingTypes = RelevantEnchants.Where(e => !highLvlHash.Contains(e.Key)).Select(e => e.Key).ToList();

                track.Tag("enchSel", "highLvl " + string.Join(',', highLevel));

                select = select.Where(a => a.Enchantments
                        .Where(e =>
                                    (ultiType == Enchantment.EnchantmentType.unknown || ultiType == e.Type && ultiLevel == e.Level)
                                    &&
                                    (minLvl1.Contains(e.Type) && e.Level == 1
                                        || minLvl2.Contains(e.Type) && e.Level == 2
                                        || minLvl3.Contains(e.Type) && e.Level == 3
                                        || minLvl4.Contains(e.Type) && e.Level == 4
                                        || minLvl5.Contains(e.Type) && e.Level == 5
                                        || minLvl6.Contains(e.Type) && e.Level == 6
                                        || minLvl7.Contains(e.Type) && e.Level == 7
                                        || minLvl8.Contains(e.Type) && e.Level == 8
                                        || minLvl9.Contains(e.Type) && e.Level == 9
                                        || minLvl10.Contains(e.Type) && e.Level == 10)
                                        && !missingTypes.Contains(e.Type)
                                    ).Count() == matchingCount
                                    && !a.Enchantments.Where(e => missingTypes.Contains(e.Type)).Any());
            }
            else if (auction.Enchantments?.Count == 1 && auction.Tag == "ENCHANTED_BOOK")
                select = select.Where(a => a.Enchantments != null && a.Enchantments.Count() == 1
                        && a.Enchantments.First().Type == auction.Enchantments.First().Type
                        && a.Enchantments.First().Level == auction.Enchantments.First().Level);
            else if (auction.Enchantments?.Count == 2 && auction.Tag == "ENCHANTED_BOOK")
            {
                track.Tag("enchSel", "2 exact ");
                select = select.Where(a => a.Enchantments != null && a.Enchantments.Count() == 2
                        && a.Enchantments.Where(e =>
                            e.Type == auction.Enchantments[0].Type && e.Level == auction.Enchantments[0].Level
                            || e.Type == auction.Enchantments[1].Type && e.Level == auction.Enchantments[1].Level).Count() == 2);
            }
            // make sure we exclude special enchants to get a reasonable price
            else if (auction.Enchantments.Any())
                select = select.Where(a => !a.Enchantments.Where(e => relevant.Contains(e.Type) || e.Level > 5).Any());
            else
                select = select.Where(a => !a.Enchantments.Any());
            return select;
        }

        private static HashSet<Enchantment.EnchantmentType> GetMinLvl(List<Enchantment> highLvlEnchantList, int lvl)
        {
            return highLvlEnchantList.Where(e => e.Level == lvl).Select(e => e.Type).ToHashSet();
        }

        internal async Task VoidReferences(SaveAuction auction)
        {
            var key = GetCacheKey(auction);
            await CacheService.Instance.DeleteInRedis(key);
        }

        private static ProducerConfig producerConfig = new ProducerConfig
        {
            BootstrapServers = SimplerConfig.Config.Instance["KAFKA_HOST"],
            LingerMs = 2
        };

        /*
        1 Enchantments
        2 Dungon Stars
        3 Skins
        4 Rarity
        5 Reforge
        6 Flumming potato books
        7 Hot Potato Books
        */
    }

    public class CurrentPrice
    {
        public double sell { get; set; }
    }

    public class FindTracking
    {
        public Activity Span;
        public Dictionary<string, string> Context = new Dictionary<string, string>();

        internal void Tag(string key, string value)
        {
            Span?.SetTag(key, value);
            Context[key] = value;
        }
    }

    [MessagePackObject]
    public class RelevantElement
    {
        [Key(0)]
        public List<SaveAuction> references;
        [Key(1)]
        public DateTime Oldest;
        [Key(2)]
        public int HitCount;
        [Key(3)]
        public DateTime QueryTime = DateTime.Now;
        [IgnoreMember]
        public string Key;
    }
}
