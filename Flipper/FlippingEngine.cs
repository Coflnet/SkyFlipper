using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using hypixel;
using MessagePack;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Newtonsoft.Json;
using OpenTracing.Propagation;

namespace Coflnet.Sky.Flipper
{
    public class FlipperEngine
    {
        public static FlipperEngine Instance { get; }

        public static readonly string ProduceTopic = SimplerConfig.Config.Instance["TOPICS:FLIP"];
        public static readonly string NewAuctionTopic = SimplerConfig.Config.Instance["TOPICS:NEW_AUCTION"];
        public static readonly string KafkaHost = SimplerConfig.Config.Instance["KAFKA_HOST"];
        public static readonly string LowPricedAuctionTopic = SimplerConfig.Config.Instance["TOPICS:LOW_PRICED"];
        /// <summary>
        /// List of ultimate enchantments
        /// </summary>
        public static ConcurrentDictionary<Enchantment.EnchantmentType, bool> UltimateEnchants = new ConcurrentDictionary<Enchantment.EnchantmentType, bool>();
        public static ConcurrentDictionary<Enchantment.EnchantmentType, byte> RelevantEnchants = new ConcurrentDictionary<Enchantment.EnchantmentType, byte>();


        private ConcurrentQueue<SaveAuction> PotetialFlipps = new ConcurrentQueue<SaveAuction>();
        private ConcurrentQueue<SaveAuction> LowPriceQueue = new ConcurrentQueue<SaveAuction>();

        static private List<Enchantment.EnchantmentType> UltiEnchantList = new List<Enchantment.EnchantmentType>();
        internal IServiceScopeFactory serviceFactory;

        Prometheus.Counter foundFlipCount = Prometheus.Metrics
                    .CreateCounter("flips_found", "Number of flips found");
        Prometheus.Counter alreadySold = Prometheus.Metrics
                    .CreateCounter("already_sold_flips", "Flips that were already sold for premium users for some reason");
        Prometheus.Histogram time = Prometheus.Metrics.CreateHistogram("time_to_find_flip", "How long did it take to find a flip", new Prometheus.HistogramConfiguration()
        {
            Buckets = Prometheus.Histogram.LinearBuckets(start: 10, width: 2, count: 10)
        });
        static Prometheus.HistogramConfiguration buckets = new Prometheus.HistogramConfiguration()
        {
            Buckets = Prometheus.Histogram.LinearBuckets(start: 10, width: 1, count: 10)
        };
        static Prometheus.Histogram runtroughTime = Prometheus.Metrics.CreateHistogram("sky_flipper_auction_to_send_flip_seconds", "Seconds from loading the auction to finding the flip. (should be close to 10)",
            buckets);
        static Prometheus.Histogram receiveTime = Prometheus.Metrics.CreateHistogram("sky_flipper_auction_receive_seconds", "Seconds that the flipper received an auction. (should be close to 10)",
            buckets);

        public DateTime LastLiveProbe = DateTime.Now;
        private SemaphoreSlim throttler = new SemaphoreSlim(25);

        static FlipperEngine()
        {
            Instance = new FlipperEngine();
            foreach (var item in Enum.GetValues(typeof(Enchantment.EnchantmentType)).Cast<Enchantment.EnchantmentType>())
            {
                if (item.ToString().StartsWith("ultimate_", true, null))
                {
                    UltimateEnchants.TryAdd(item, true);
                    UltiEnchantList.Add(item);
                }
            }
            foreach (var item in Coflnet.Sky.Constants.RelevantEnchants)
            {
                RelevantEnchants.AddOrUpdate(item.Type, item.Level, (k, o) => item.Level);
            }
        }

        public FlipperEngine()
        {
            // results are not relevant if older than 5 seconds
            commandsClient.Timeout = 5000;
        }

        private RestSharp.RestClient commandsClient = new RestSharp.RestClient("http://" + SimplerConfig.Config.Instance["skycommands_host"]);


        public Task ProcessPotentialFlipps()
        {
            return ProcessPotentialFlipps(CancellationToken.None);
        }

        public async Task ProcessPotentialFlipps(CancellationToken cancleToken)
        {
            try
            {
                var conf = new ConsumerConfig
                {
                    GroupId = "flipper-processor",
                    BootstrapServers = KafkaHost,
                    // Note: The AutoOffsetReset property determines the start offset in the event
                    // there are not yet any committed offsets for the consumer group for the
                    // topic/partitions of interest. By default, offsets are committed
                    // automatically, so in this example, consumption will only start from the
                    // earliest message in the topic 'my-topic' the first time you run the program.
                    AutoOffsetReset = AutoOffsetReset.Earliest,
                    AutoCommitIntervalMs = 1500
                };

                using (var c = new ConsumerBuilder<Ignore, SaveAuction>(conf).SetValueDeserializer(SerializerFactory.GetDeserializer<SaveAuction>()).Build())
                using (var lpp = new ProducerBuilder<string, LowPricedAuction>(producerConfig).SetValueSerializer(SerializerFactory.GetSerializer<LowPricedAuction>()).Build())
                using (var p = new ProducerBuilder<string, FlipInstance>(producerConfig).SetValueSerializer(SerializerFactory.GetSerializer<FlipInstance>()).Build())
                {
                    c.Subscribe(NewAuctionTopic);
                    try
                    {
                        await Task.Yield();
                        Console.WriteLine("starting worker");
                        var taskFactory = new TaskFactory(new LimitedConcurrencyLevelTaskScheduler(2));
                        while (!cancleToken.IsCancellationRequested)
                        {
                            try
                            {
                                LastLiveProbe = DateTime.Now;
                                var cr = c.Consume(cancleToken);
                                if (cr == null)
                                    continue;
                                var tracer = OpenTracing.Util.GlobalTracer.Instance;
                                OpenTracing.ISpanContext parent;
                                if (cr.Message.Value.TraceContext == null)
                                    parent = tracer.BuildSpan("mock").StartActive().Span.Context;
                                else
                                    parent = tracer.Extract(BuiltinFormats.TextMap, cr.Message.Value.TraceContext);

                                var receiveSpan = tracer.BuildSpan("ReceiveAuction")
                                        .AsChildOf(parent).Start();
                                if (cr.Message.Value.Context != null)
                                    cr.Message.Value.Context["frec"] = (DateTime.Now - cr.Message.Value.FindTime).ToString();

                                var findingTask = taskFactory.StartNew(async () =>
                                {
                                    receiveSpan.Finish();

                                    var span = tracer.BuildSpan("SearchFlip")
                                            .WithTag("uuid", cr.Message.Value.Uuid);
                                    using var scope = span.StartActive();
                                    await throttler.WaitAsync();
                                    try
                                    {
                                        await ProcessSingleFlip(p, cr, lpp, scope);
                                    }
                                    catch (Exception e)
                                    {
                                        dev.Logger.Instance.Error(e, "flip search");
                                        scope.Span.SetTag("error", "true").Log(e.Message).Log(e.StackTrace);
                                    }
                                    finally
                                    {
                                        throttler.Release();
                                    }
                                });

                                //c.Commit(new TopicPartitionOffset[] { cr.TopicPartitionOffset });
                                if (cr.Offset.Value % 500 == 0)
                                {
                                    Console.WriteLine($"consumed new-auction {cr.Offset.Value}");
                                    if (cr.Offset.Value % 2000 == 0)
                                        System.GC.Collect();
                                }
                            }
                            catch (ConsumeException e)
                            {
                                dev.Logger.Instance.Error(e, "flipper process potential ");
                            }
                        }
                    }
                    catch (OperationCanceledException)
                    {
                        // Ensure the consumer leaves the group cleanly and final offsets are committed.
                        c.Close();
                    }
                }

            }
            catch (Exception e)
            {
                dev.Logger.Instance.Error($"Flipper threw an exception {e.Message} {e.StackTrace}");
            }
        }

        private async Task ProcessSingleFlip(IProducer<string, FlipInstance> p, ConsumeResult<Ignore, SaveAuction> cr, IProducer<string, LowPricedAuction> lpp, OpenTracing.IScope span)
        {
            receiveTime.Observe((DateTime.Now - cr.Message.Value.FindTime).TotalSeconds);

            var startTime = DateTime.Now;
            FlipInstance flip = null;

            flip = await NewAuction(cr.Message.Value, lpp, span);

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
        private bool GetAuctionToCheckFlipability(out SaveAuction auction)
        {
            // mix in lowerPrice
            if (_auctionCounter++ % 3 != 0)
                if (PotetialFlipps.TryDequeue(out auction))
                    return true;
            return LowPriceQueue.TryDequeue(out auction);
        }

        public ConcurrentDictionary<long, List<long>> relevantAuctionIds = new ConcurrentDictionary<long, List<long>>();

        public async System.Threading.Tasks.Task<FlipInstance> NewAuction(SaveAuction auction, IProducer<string, LowPricedAuction> lpp, OpenTracing.IScope span)
        {
            // blacklist
            if (auction.ItemName == "null")
                return null;

            // makes no sense to check old auctions
            if (auction.Start < DateTime.Now - TimeSpan.FromMinutes(5))
                return null;

            var price = (auction.HighestBidAmount == 0 ? auction.StartingBid : (auction.HighestBidAmount * 1.1)) / auction.Count;
            if (price < 10) // only care about auctions worth more than the fee
                return null;


            if (auction.NBTLookup == null || auction.NBTLookup.Count() == 0)
                auction.NBTLookup = NBT.CreateLookup(auction);

            var trackingContext = new FindTracking() { Span = span.Span };
            var referenceElement = await GetRelevantAuctionsCache(auction, trackingContext);
            var relevantAuctions = referenceElement.references;

            long medianPrice = 0;
            if (relevantAuctions.Count < 2)
            {
                Console.WriteLine($"Could not find enough relevant auctions for {auction.ItemName} {auction.Uuid} ({ExtractRelevantEnchants(auction).Count} {relevantAuctions.Count})");

                // the overall median was deemed to inaccurate
                return null;
                /* var itemId = ItemDetails.Instance.GetItemIdForName(auction.Tag, false);
                 var lookupPrices = await ItemPrices.GetLookupForToday(itemId);
                 if (lookupPrices?.Prices.Count > 0)
                     medianPrice = (long)(lookupPrices?.Prices?.Average(p => p.Avg * 0.8 + p.Min * 0.2) ?? 0);*/
            }
            else
            {
                medianPrice = (await Task.WhenAll(relevantAuctions
                                //.OrderByDescending(a => a.HighestBidAmount)
                                .Select(async a => a.HighestBidAmount / (a.Count == 0 ? 1 : a.Count) / (a.Count == auction.Count ? 1 : 2) - await GetGemstoneWorth(a))))
                                .OrderByDescending(a => a)
                                .Skip(relevantAuctions.Count / 2)
                                .FirstOrDefault();
            }
            var reductionDueToCount = Math.Pow(1.01, referenceElement.HitCount);
            int additionalWorth = await GetGemstoneWorth(auction);
            var recomendedBuyUnder = (medianPrice * 0.9 + additionalWorth) / reductionDueToCount;
            if (recomendedBuyUnder < 1_000_000)
            {
                recomendedBuyUnder *= 0.9;
            }

            if (price > recomendedBuyUnder || recomendedBuyUnder < 100_000) // at least 10% profit
            {
                return null; // not a good flip
            }

            // recheck with gemstone value removed (value doesn't have to be checked for all items)


            relevantAuctionIds[auction.UId] = relevantAuctions.Select(a => a.UId == 0 ? AuctionService.Instance.GetId(a.Uuid) : a.UId).ToList();
            if (relevantAuctionIds.Count > 10000)
            {
                relevantAuctionIds.Clear();
            }

            var additionalProps = new Dictionary<string, string>(trackingContext.Context);
            if (additionalWorth > 0)
            {
                additionalProps["worth"] = additionalWorth.ToString();
            }
            if (auction.Context != null)
                auction.Context["fsend"] = (DateTime.Now - auction.FindTime).ToString();

            var lowPrices = new LowPricedAuction()
            {
                Auction = auction,
                DailyVolume = (float)(relevantAuctions.Count / (DateTime.Now - referenceElement.Oldest).TotalDays),
                Finder = LowPricedAuction.FinderType.FLIPPER,
                TargetPrice = (int)medianPrice * auction.Count + additionalWorth,
                AdditionalProps = additionalProps
            };

            lpp.Produce(LowPricedAuctionTopic, new Message<string, LowPricedAuction>()
            {
                Key = auction.Uuid,
                Value = lowPrices
            });
            await SaveHitOnFlip(referenceElement, auction);

            var itemTag = auction.Tag;
            var name = PlayerSearch.Instance.GetNameWithCacheAsync(auction.AuctioneerId);
            var filters = new Dictionary<string, string>();
            var ulti = auction.Enchantments.Where(e => UltimateEnchants.ContainsKey(e.Type)).FirstOrDefault();
            if (ulti != null)
            {
                filters["Enchantment"] = ulti.Type.ToString();
                filters["EnchantLvl"] = ulti.Level.ToString();
            }
            if (relevantReforges.Contains(auction.Reforge))
            {
                filters["Reforge"] = auction.Reforge.ToString();
            }
            filters["Rarity"] = auction.Tier.ToString();

            var exactLowestTask = ItemPrices.GetLowestBin(itemTag, filters);
            List<ItemPrices.AuctionPreview> lowestBin = await ItemPrices.GetLowestBin(itemTag, auction.Tier);
            var exactLowest = await exactLowestTask;
            if (exactLowest?.Count > 1)
            {
                lowestBin = exactLowest;
            }
            // prevent price manipulation
            if (lowestBin?.FirstOrDefault()?.Price > medianPrice)
            {
                span.Span.SetTag("error", true).Log("lbin higher than median " + lowestBin?.FirstOrDefault()?.Uuid);
                return null;
            }

            var flip = new FlipInstance()
            {
                MedianPrice = (int)medianPrice * auction.Count + additionalWorth,
                Name = auction.ItemName,
                Uuid = auction.Uuid,
                LastKnownCost = (int)price * auction.Count,
                Volume = (float)(relevantAuctions.Count / (DateTime.Now - referenceElement.Oldest).TotalDays),
                Tag = auction.Tag,
                Bin = auction.Bin,
                UId = auction.UId,
                Rarity = auction.Tier,
                Interesting = PropertiesSelector.GetProperties(auction).OrderByDescending(a => a.Rating).Select(a => a.Value).ToList(),
                SellerName = await name,
                LowestBin = lowestBin?.FirstOrDefault()?.Price + additionalWorth,
                SecondLowestBin = lowestBin?.Count >= 2 ? lowestBin[1].Price : 0L,
                Auction = auction
            };

            foundFlipCount.Inc();

            time.Observe((DateTime.Now - auction.FindTime).TotalSeconds);

            return flip;
        }

        private async Task SaveHitOnFlip(RelevantElement referenceElement, SaveAuction auction)
        {

            if (referenceElement.HitCount % 5 == 1)
                Console.WriteLine($"hit {referenceElement.Key} {referenceElement.HitCount} times");

            var storeTime = DateTime.Now - referenceElement.QueryTime + TimeSpan.FromHours(2);
            if (storeTime < TimeSpan.Zero)
                storeTime = TimeSpan.FromSeconds(1);
            referenceElement.HitCount++;
            if (storeTime < TimeSpan.FromHours(1) && referenceElement.HitCount > 1)
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
            else
                // is set to fireand forget (will return imediately)
                await CacheService.Instance.SaveInRedis(referenceElement.Key, referenceElement, storeTime);
        }

        private async Task<int> GetGemstoneWorth(SaveAuction auction)
        {
            var additionalWorth = 0;
            var gems = auction.FlatenedNBT.Where(n => n.Value == "PERFECT" || n.Value == "FLAWLESS");
            if (gems.Any())
            {
                var selects = gems.Select(async (g) =>
                {
                    try
                    {
                        var type = g.Key.Split("_").First();
                        if (type == "COMBAT" || type == "DEFENSIVE" || type == "UNIVERSAL")
                            type = auction.FlatenedNBT[g.Key + "_gem"];
                        var route = $"/api/item/price/{g.Value}_{type}_GEM/current";
                        var result = await commandsClient.ExecuteGetAsync(new RestSharp.RestRequest(route));
                        var profit = JsonConvert.DeserializeObject<CurrentPrice>(result.Content).sell;
                        if (g.Key == "PERFECT")
                            return profit - 500_000;
                        return profit - 100_000;
                    }
                    catch(Exception e)
                    {
                        dev.Logger.Instance.Error(e,"retrieving gem price");
                        return 0;
                    }
                });
                foreach (var item in selects)
                {
                    additionalWorth += Convert.ToInt32(await item);
                }
            }

            return additionalWorth;
        }

        private static HashSet<string> ignoredNbt = new HashSet<string>()
                { "uid", "spawnedFor", "bossId", "exp", "uuid" };
        /// <summary>
        /// Gets relevant items for an auction, checks cache first
        /// </summary>
        /// <param name="auction"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public async Task<RelevantElement> GetRelevantAuctionsCache(SaveAuction auction, FindTracking tracking)
        {
            var key = $"o{auction.ItemId}{auction.ItemName}{auction.Tier}{auction.Bin}{auction.Count}";
            if (relevantReforges.Contains(auction.Reforge))
                key += auction.Reforge;
            var relevant = ExtractRelevantEnchants(auction);
            if (relevant.Count() == 0)
                key += String.Concat(auction.Enchantments.Select(a => $"{a.Type}{a.Level}"));
            else
                key += String.Concat(relevant.Select(a => $"{a.Type}{a.Level}"));
            key += String.Concat(auction.FlatenedNBT.Where(d => !ignoredNbt.Contains(d.Key)));
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

        private async Task<RelevantElement> GetAndCacheReferenceAuctions(SaveAuction auction, FindTracking tracking, string key)
        {
            using var scope = serviceFactory.CreateScope();
            using var context = scope.ServiceProvider.GetRequiredService<HypixelContext>();

            var referenceAuctions = await GetRelevantAuctions(auction, context, tracking);
            // shifted out of the critical path
            if (referenceAuctions.references.Count > 1)
            {
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
            var itemId = ItemDetails.Instance.GetItemIdForName(auction.Tag, false);
            var youngest = DateTime.Now;
            List<Enchantment> relevantEnchants = ExtractRelevantEnchants(auction);
            //var matchingCount = relevantEnchants.Count > 3 ? relevantEnchants.Count * 2 / 3 : relevantEnchants.Count;
            var ulti = relevantEnchants.Where(e => UltimateEnchants.ContainsKey(e.Type)).FirstOrDefault();
            var highLvlEnchantList = relevantEnchants.Where(e => !UltimateEnchants.ContainsKey(e.Type)).Select(a => a.Type).ToList();
            var oldest = DateTime.Now - TimeSpan.FromHours(2);

            IQueryable<SaveAuction> select = GetSelect(auction, context, clearedName, itemId, youngest, ulti, relevantEnchants, oldest, tracking, 10);

            var relevantAuctions = await select
                .ToListAsync();

            if (relevantAuctions.Count < 11)
            {
                // to few auctions in last hour, try a whole day
                oldest = DateTime.Now - TimeSpan.FromDays(1.5);
                relevantAuctions = await GetSelect(auction, context, clearedName, itemId, youngest, ulti, relevantEnchants, oldest, tracking, 90)
                .ToListAsync();

                if (relevantAuctions.Count < 50 && PotetialFlipps.Count < 2000)
                {
                    // to few auctions in a day, query a week
                    youngest = oldest;
                    oldest = DateTime.Now - TimeSpan.FromDays(8);
                    relevantAuctions.AddRange(await GetSelect(auction, context, clearedName, itemId, youngest, ulti, relevantEnchants, oldest, tracking, 120)
                    .ToListAsync());
                    if (relevantAuctions.Count < 10)
                    {
                        youngest = DateTime.Now;
                        clearedName = clearedName.Replace("✪", "").Trim();
                        relevantAuctions = await GetSelect(auction, context, clearedName, itemId, youngest, ulti, relevantEnchants, oldest, tracking, 120, true)
                        .ToListAsync();
                    }
                }
            }

            /* got replaced with average overall lookup
            if (relevantAuctions.Count < 3 && PotetialFlipps.Count < 100)
            {
                oldest = DateTime.Now - TimeSpan.FromDays(25);
                relevantAuctions = await GetSelect(auction, context, null, itemId, youngest, matchingCount, ulti, ultiList, highLvlEnchantList, oldest)
                        .ToListAsync();
            } */
            if (relevantAuctions.Count > 1)
                relevantAuctions = relevantAuctions.GroupBy(a => a.SellerId).Select(a => a.First()).ToList();


            return new RelevantElement()
            {
                references = relevantAuctions,
                Oldest = oldest
            };
        }

        public static List<Enchantment> ExtractRelevantEnchants(SaveAuction auction)
        {
            return auction.Enchantments?.Where(e => (!RelevantEnchants.ContainsKey(e.Type) && e.Level >= 6) || (RelevantEnchants.TryGetValue(e.Type, out byte lvl)) && e.Level >= lvl)
                .ToList();
        }

        private readonly static HashSet<ItemReferences.Reforge> relevantReforges = Coflnet.Sky.Constants.RelevantReforges;

        private static IQueryable<SaveAuction> GetSelect(
            SaveAuction auction,
            HypixelContext context,
            string clearedName,
            int itemId,
            DateTime youngest,
            Enchantment ulti,
            List<Enchantment> highLvlEnchantList,
            DateTime oldest,
            FindTracking track,
            int limit = 60,
            bool reduced = false)
        {
            var select = context.Auctions
                .Where(a => a.ItemId == itemId)
                .Where(a => a.HighestBidAmount > 0)
                .Where(a => a.Tier == auction.Tier);

            byte ultiLevel = 127;
            var flatNbt = auction.FlatenedNBT ?? new Dictionary<string, string>();
            Enchantment.EnchantmentType ultiType = Enchantment.EnchantmentType.unknown;
            if (ulti != null)
            {
                ultiLevel = ulti.Level;
                ultiType = ulti.Type;
            }

            if (relevantReforges.Contains(auction.Reforge))
                select = select.Where(a => a.Reforge == auction.Reforge);
            else
                select = select.Where(a => !relevantReforges.Contains(a.Reforge));

            if (auction.Count > 1 && !reduced) // try to match exact count
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
            if (flatNbt.ContainsKey("edition"))
                select = AddNbtRangeSelect(ref select, flatNbt, "edition", 100, 10);

            if (flatNbt.ContainsKey("new_years_cake"))
                select = AddNBTSelect(select, flatNbt, "new_years_cake");
            if (flatNbt.ContainsKey("dungeon_item_level") && !reduced)
                select = AddNBTSelect(select, flatNbt, "dungeon_item_level");
            if (flatNbt.ContainsKey("candyUsed"))
                select = AddCandySelect(select, flatNbt, "candyUsed");
            if (flatNbt.ContainsKey("art_of_war_count"))
                select = AddNBTSelect(select, flatNbt, "art_of_war_count");



            if (flatNbt.ContainsKey("heldItem"))
            {
                var keyId = NBT.GetLookupKey("heldItem");
                var val = ItemDetails.Instance.GetItemIdForName(flatNbt["heldItem"]);
                select = select.Where(a => a.NBTLookup.Where(n => n.KeyId == keyId && n.Value == val).Any());
            }
            else if (flatNbt.ContainsKey("candyUsed")) // every pet has candyUsed attribute
            {
                var keyId = NBT.GetLookupKey("heldItem");
                select = select.Where(a => !a.NBTLookup.Where(n => n.KeyId == keyId).Any());
            }

            if (flatNbt.ContainsKey("skin"))
            {
                var keyId = NBT.GetLookupKey("skin");
                var val = NBT.GetItemIdForSkin(flatNbt["skin"]);
                select = select.Where(a => a.NBTLookup.Where(n => n.KeyId == keyId && n.Value == val).Any());
            }

            if (flatNbt.ContainsKey("color"))
            {
                var keyId = NBT.GetLookupKey("color");
                var val = NBT.GetColor(flatNbt["color"]);
                select = select.Where(a => a.NBTLookup.Where(n => n.KeyId == keyId && n.Value == val).Any());
            }

            foreach (var item in flatNbt)
            {
                if (item.Key.EndsWith("_kills"))
                {
                    var keyId = NBT.GetLookupKey(item.Key);
                    var val = Int32.Parse(item.Value);
                    var max = val * 1.2;
                    var min = val * 0.8;
                    select = select.Where(a => a.NBTLookup.Where(n => n.KeyId == keyId && n.Value >= min && n.Value < max).Any());
                }
            }



            bool canHaveGemstones = auction.Tag.StartsWith("DIVAN")
                            || auction.Tag == "GEMSTONE_GAUNTLET";
            if (canHaveGemstones
                || flatNbt.ContainsKey("unlocked_slots"))
                select = AddNBTSelect(select, flatNbt, "unlocked_slots");

            if (canHaveGemstones || flatNbt.ContainsKey("gemstone_slots"))
                select = AddNBTSelect(select, flatNbt, "gemstone_slots");

            select = AddEnchantmentSubselect(auction, highLvlEnchantList, select, ultiLevel, ultiType, track, reduced);
            if (limit == 0)
                return select;

            return select
                .Where(a => a.End > oldest && a.End < youngest)
                //.OrderByDescending(a=>a.Id)
                .Include(a => a.NbtData)
                .Take(limit);
        }

        private static IQueryable<SaveAuction> AddNBTSelect(IQueryable<SaveAuction> select, Dictionary<string, string> flatNbt, string keyValue)
        {
            var keyId = NBT.GetLookupKey(keyValue);
            if (!flatNbt.ContainsKey(keyValue))
                return select.Where(a => !a.NBTLookup.Where(n => n.KeyId == keyId).Any());

            if (!long.TryParse(flatNbt[keyValue], out long val))
                val = NBT.Instance.GetValueId(keyId, flatNbt[keyValue]);
            select = select.Where(a => a.NBTLookup.Where(n => n.KeyId == keyId && n.Value == val).Any());
            return select;
        }

        private static IQueryable<SaveAuction> AddCandySelect(IQueryable<SaveAuction> select, Dictionary<string, string> flatNbt, string keyValue)
        {
            var keyId = NBT.GetLookupKey(keyValue);
            long.TryParse(flatNbt[keyValue], out long val);
            if (val > 0)
                return select.Where(a => a.NBTLookup.Where(n => n.KeyId == keyId && n.Value > 0).Any());
            return select.Where(a => a.NBTLookup.Where(n => n.KeyId == keyId && n.Value == 0).Any());
        }

        private static IQueryable<SaveAuction> AddMidasSelect(IQueryable<SaveAuction> select, Dictionary<string, string> flatNbt, string keyValue)
        {
            var maxDiff = 2_000_000;
            return AddNbtRangeSelect(ref select, flatNbt, keyValue, maxDiff);
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
        private static IQueryable<SaveAuction> AddNbtRangeSelect(ref IQueryable<SaveAuction> select, Dictionary<string, string> flatNbt, string keyValue, long maxDiff, int percentIncrease = 0)
        {
            var val = long.Parse(flatNbt[keyValue]);
            var keyId = NBT.GetLookupKey(keyValue);
            maxDiff += val * percentIncrease;
            select = select.Where(a => a.NBTLookup.Where(n => n.KeyId == keyId && n.Value > val - maxDiff && n.Value < val + maxDiff).Any());
            return select;
        }

        private static IQueryable<SaveAuction> AddPetLvlSelect(SaveAuction auction, IQueryable<SaveAuction> select)
        {
            var sb = new StringBuilder(auction.ItemName);
            if (sb[6] == ']')
                sb[5] = '_';
            else
                sb[6] = '_';
            select = select.Where(a => EF.Functions.Like(a.ItemName, sb.ToString()));
            return select;
        }

        public static IQueryable<SaveAuction> AddEnchantmentSubselect(SaveAuction auction, List<Enchantment> highLvlEnchantList, IQueryable<SaveAuction> select, byte ultiLevel, Enchantment.EnchantmentType ultiType, FindTracking track, bool reduced = false)
        {
            var relevant = RelevantEnchants.Select(r => r.Key).ToList();
            if (highLvlEnchantList.Count > 0)
            {
                var matchingCount = highLvlEnchantList.Count;
                if (reduced) // reduce to find match
                    highLvlEnchantList = highLvlEnchantList.Where(e => e.Level > 2).ToList();
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
        public OpenTracing.ISpan Span;
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
