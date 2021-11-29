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
            Buckets = Prometheus.Histogram.LinearBuckets(start: 10, width: 2, count: 10)
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
                Console.WriteLine(item.Type.ToString() + " " + item.Level);
            }

        }


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

                                var span = tracer.BuildSpan("SearchFlip")
                                        .AsChildOf(parent);
                                var receiveSpan = tracer.BuildSpan("ReceiveAuction")
                                        .AsChildOf(parent).Start();
                                var findingTask = taskFactory.StartNew(async () =>
                                {
                                    receiveSpan.Finish();
                                    using var scope = span.StartActive();
                                    await throttler.WaitAsync();
                                    try
                                    {
                                        await ProcessSingleFlip(p, cr, lpp);
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

        private async Task ProcessSingleFlip(IProducer<string, FlipInstance> p, ConsumeResult<Ignore, SaveAuction> cr, IProducer<string, LowPricedAuction> lpp)
        {
            receiveTime.Observe((DateTime.Now - cr.Message.Value.FindTime).TotalSeconds);


            FlipInstance flip = null;
            using (var scope = serviceFactory.CreateScope())
            using (var context = scope.ServiceProvider.GetRequiredService<HypixelContext>())
            {
                flip = await NewAuction(cr.Message.Value, context, lpp);
            }
            if (flip != null)
            {
                var timetofind = (DateTime.Now - flip.Auction.FindTime).TotalSeconds;
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

        public async System.Threading.Tasks.Task<FlipInstance> NewAuction(SaveAuction auction, HypixelContext context, IProducer<string, LowPricedAuction> lpp)
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

            var (relevantAuctions, oldest) = await GetRelevantAuctionsCache(auction, context);

            long medianPrice = 0;
            if (relevantAuctions.Count < 2)
            {
                Console.WriteLine($"Could not find enough relevant auctions for {auction.ItemName} {auction.Uuid} ({auction.Enchantments.Count} {relevantAuctions.Count})");

                // the overall median was deemed to inaccurate
                return null;
                /* var itemId = ItemDetails.Instance.GetItemIdForName(auction.Tag, false);
                 var lookupPrices = await ItemPrices.GetLookupForToday(itemId);
                 if (lookupPrices?.Prices.Count > 0)
                     medianPrice = (long)(lookupPrices?.Prices?.Average(p => p.Avg * 0.8 + p.Min * 0.2) ?? 0);*/
            }
            else
            {
                medianPrice = relevantAuctions
                                .OrderByDescending(a => a.HighestBidAmount)
                                .Select(a => a.HighestBidAmount / (a.Count == 0 ? 1 : a.Count))
                                .Skip(relevantAuctions.Count / 2)
                                .FirstOrDefault();
            }

            var recomendedBuyUnder = medianPrice * 0.9;
            if (recomendedBuyUnder < 1_000_000)
            {
                recomendedBuyUnder *= 0.9;
            }
            if (price > recomendedBuyUnder) // at least 10% profit
            {
                return null; // not a good flip
            }

            relevantAuctionIds[auction.UId] = relevantAuctions.Select(a => a.UId == 0 ? AuctionService.Instance.GetId(a.Uuid) : a.UId).ToList();
            if (relevantAuctionIds.Count > 10000)
            {
                relevantAuctionIds.Clear();
            }

            var lowPrices = new LowPricedAuction()
            {
                Auction = auction,
                DailyVolume = (float)(relevantAuctions.Count / (DateTime.Now - oldest).TotalDays),
                Finder = LowPricedAuction.FinderType.FLIPPER,
                TargetPrice = (int)medianPrice * auction.Count
            };

            lpp.Produce(LowPricedAuctionTopic, new Message<string, LowPricedAuction>()
            {
                Key = auction.Uuid,
                Value = lowPrices
            });


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

            var flip = new FlipInstance()
            {
                MedianPrice = (int)medianPrice * auction.Count,
                Name = auction.ItemName,
                Uuid = auction.Uuid,
                LastKnownCost = (int)price * auction.Count,
                Volume = (float)(relevantAuctions.Count / (DateTime.Now - oldest).TotalDays),
                Tag = auction.Tag,
                Bin = auction.Bin,
                UId = auction.UId,
                Rarity = auction.Tier,
                Interesting = PropertiesSelector.GetProperties(auction).OrderByDescending(a => a.Rating).Select(a => a.Value).ToList(),
                SellerName = await name,
                LowestBin = lowestBin?.FirstOrDefault()?.Price,
                SecondLowestBin = lowestBin?.Count >= 2 ? lowestBin[1].Price : 0L,
                Auction = auction
            };

            foundFlipCount.Inc();

            time.Observe((DateTime.Now - auction.FindTime).TotalSeconds);
            return flip;
        }


        private static HashSet<string> ignoredNbt = new HashSet<string>() 
                { "uid", "spawnedFor", "bossId", "exp", "uuid" };
        /// <summary>
        /// Gets relevant items for an auction, checks cache first
        /// </summary>
        /// <param name="auction"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public async Task<(List<SaveAuction>, DateTime)> GetRelevantAuctionsCache(SaveAuction auction, HypixelContext context)
        {
            var key = $"n{auction.ItemId}{auction.ItemName}{auction.Tier}{auction.Bin}{auction.Count}";
            var relevant = ExtractRelevantEnchants(auction);
            if (relevant.Count() == 0)
                key += String.Concat(auction.Enchantments.Select(a => $"{a.Type}{a.Level}"));
            else
                key += String.Concat(relevant.Select(a => $"{a.Type}{a.Level}"));
            key += String.Concat(auction.FlatenedNBT.Where(d => !ignoredNbt.Contains(d.Key)));
            try
            {
                var fromCache = await CacheService.Instance.GetFromRedis<(List<SaveAuction>, DateTime)>(key);
                if (fromCache != default((List<SaveAuction>, DateTime)))
                {
                    //Console.WriteLine("flip cache hit");
                    return fromCache;
                }
            }
            catch (Exception e)
            {
                dev.Logger.Instance.Error(e, "cache flip");
            }

            var referenceAuctions = await GetRelevantAuctions(auction, context);
            // shifted out of the critical path
            if (referenceAuctions.Item1.Count > 1)
            {
                var saveTask = CacheService.Instance.SaveInRedis<(List<SaveAuction>, DateTime)>(key, referenceAuctions, TimeSpan.FromHours(1));
            }
            return referenceAuctions;
        }

        private async Task<(List<SaveAuction>, DateTime)> GetRelevantAuctions(SaveAuction auction, HypixelContext context)
        {
            var itemData = auction.NbtData.Data;
            var clearedName = auction.Reforge != ItemReferences.Reforge.None ? ItemReferences.RemoveReforge(auction.ItemName) : auction.ItemName;
            var itemId = ItemDetails.Instance.GetItemIdForName(auction.Tag, false);
            var youngest = DateTime.Now;
            List<Enchantment> relevantEnchants = ExtractRelevantEnchants(auction);
            var matchingCount = relevantEnchants.Count > 3 ? relevantEnchants.Count * 2 / 3 : relevantEnchants.Count;
            var ulti = relevantEnchants.Where(e => UltimateEnchants.ContainsKey(e.Type)).FirstOrDefault();
            var highLvlEnchantList = relevantEnchants.Where(e => !UltimateEnchants.ContainsKey(e.Type)).Select(a => a.Type).ToList();
            var oldest = DateTime.Now - TimeSpan.FromHours(1);

            IQueryable<SaveAuction> select = GetSelect(auction, context, clearedName, itemId, youngest, matchingCount, ulti, relevantEnchants, oldest, auction.Reforge, 10);

            var relevantAuctions = await select
                .ToListAsync();

            if (relevantAuctions.Count < 9)
            {
                // to few auctions in last hour, try a whole day
                oldest = DateTime.Now - TimeSpan.FromDays(1.5);
                relevantAuctions = await GetSelect(auction, context, clearedName, itemId, youngest, matchingCount, ulti, relevantEnchants, oldest, auction.Reforge, 90)
                .ToListAsync();

                if (relevantAuctions.Count < 50 && PotetialFlipps.Count < 2000)
                {
                    // to few auctions in a day, query a week
                    youngest = oldest;
                    oldest = DateTime.Now - TimeSpan.FromDays(8);
                    relevantAuctions.AddRange(await GetSelect(auction, context, clearedName, itemId, youngest, matchingCount, ulti, relevantEnchants, oldest, auction.Reforge, 120)
                    .ToListAsync());
                    if (relevantAuctions.Count < 10 && clearedName.Contains("✪"))
                    {
                        youngest = DateTime.Now;
                        clearedName = clearedName.Replace("✪", "").Trim();
                        relevantAuctions = await GetSelect(auction, context, clearedName, itemId, youngest, matchingCount, ulti, relevantEnchants, oldest, auction.Reforge, 120)
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


            return (relevantAuctions, oldest);
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
            int matchingCount,
            Enchantment ulti,
            List<Enchantment> highLvlEnchantList,
            DateTime oldest,
            ItemReferences.Reforge reforge,
            int limit = 60)
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

            if (relevantReforges.Contains(reforge))
                select = select.Where(a => a.Reforge == reforge);
            else
                select = select.Where(a => !relevantReforges.Contains(a.Reforge));


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


            if (flatNbt.ContainsKey("rarity_upgrades"))
                select = AddNBTSelect(select, flatNbt, "rarity_upgrades");
            if (auction.Tag == "ASPECT_OF_THE_VOID" || auction.Tag == "ASPECT_OF_THE_END")
                select = AddNBTSelect(select, flatNbt, "ethermerge");
            if (flatNbt.ContainsKey("edition"))
                select = AddNbtRangeSelect(ref select, flatNbt, "edition", 100, 10);

            if (flatNbt.ContainsKey("new_years_cake"))
                select = AddNBTSelect(select, flatNbt, "new_years_cake");
            if (flatNbt.ContainsKey("dungeon_item_level"))
                select = AddNBTSelect(select, flatNbt, "dungeon_item_level");



            if (flatNbt.ContainsKey("heldItem"))
            {
                var keyId = NBT.GetLookupKey("heldItem");
                var val = ItemDetails.Instance.GetItemIdForName(flatNbt["heldItem"]);
                select = select.Where(a => a.NBTLookup.Where(n => n.KeyId == keyId && n.Value == val).Any());
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

            select = AddEnchantmentSubselect(auction, matchingCount, highLvlEnchantList, select, ultiLevel, ultiType);
            if (limit == 0)
                return select;

            return select
                .Where(a => a.End > oldest && a.End < youngest)
                //.OrderByDescending(a=>a.Id)
                //.Include(a => a.NbtData)
                .Take(limit);
        }

        private static IQueryable<SaveAuction> AddNBTSelect(IQueryable<SaveAuction> select, Dictionary<string, string> flatNbt, string keyValue)
        {
            var keyId = NBT.GetLookupKey(keyValue);
            if (!flatNbt.ContainsKey(keyValue))
                return select.Where(a => !a.NBTLookup.Where(n => n.KeyId == keyId).Any());

            if (!long.TryParse(flatNbt[keyValue], out long val))
                val = NBT.GetValueId(keyId, flatNbt[keyValue]);
            select = select.Where(a => a.NBTLookup.Where(n => n.KeyId == keyId && n.Value == val).Any());
            return select;
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

        private static IQueryable<SaveAuction> AddEnchantmentSubselect(SaveAuction auction, int matchingCount, List<Enchantment> highLvlEnchantList, IQueryable<SaveAuction> select, byte ultiLevel, Enchantment.EnchantmentType ultiType)
        {

            var relevant = RelevantEnchants.Select(r => r.Key).ToList();
            matchingCount = highLvlEnchantList.Count > 1 ? highLvlEnchantList.Count - 1 : 1;
            if (highLvlEnchantList.Count == 2)
                matchingCount = 2;
            if (highLvlEnchantList.Count() > 0)
            {
                var maxImportantEnchants = highLvlEnchantList.Count() + 1 + (ultiType == Enchantment.EnchantmentType.unknown ? 0 : 1);
                var highLevel = highLvlEnchantList.Select(r => new { r.Type, r.Level }).ToList();
                var highLvlHash = highLvlEnchantList.Select(r => r.Type).ToHashSet();

                var minLvl1 = GetMinLvl(highLvlEnchantList, 1);
                var minLvl2 = GetMinLvl(highLvlEnchantList, 2);
                var minLvl3 = GetMinLvl(highLvlEnchantList, 3);
                var minLvl4 = GetMinLvl(highLvlEnchantList, 4);
                var minLvl5 = GetMinLvl(highLvlEnchantList, 5);
                var minLvl6 = GetMinLvl6(auction);
                var minLvl7 = GetMinLvl(highLvlEnchantList, 7);

                select = select.Where(a => a.Enchantments
                        .Where(e => //relevant.Where(r => r.Key == e.Type && e.Level >= r.Value).Any() &&  // relevant enchant of references
                                    //highLevel.Where(r => r.Type == e.Type && e.Level == r.Level).Any()
                                    (ultiType == Enchantment.EnchantmentType.unknown || ultiType == e.Type && ultiLevel == e.Level)
                                    &&
                                    (minLvl1.Contains(e.Type) && e.Level == 1
                                        || minLvl2.Contains(e.Type) && e.Level == 2
                                        || minLvl3.Contains(e.Type) && e.Level == 3
                                        || minLvl4.Contains(e.Type) && e.Level == 4
                                        || minLvl5.Contains(e.Type) && e.Level == 5
                                        || minLvl6.Contains(e.Type) && e.Level == 6
                                        || minLvl7.Contains(e.Type) && e.Level == 7)
                                    ).Count() >= matchingCount);
            }
            else if (auction.Enchantments?.Count == 1 && auction.Tag == "ENCHANTED_BOOK")
                select = select.Where(a => a.Enchantments != null && a.Enchantments.Count() == 1
                        && a.Enchantments.First().Type == auction.Enchantments.First().Type
                        && a.Enchantments.First().Level == auction.Enchantments.First().Level);
            else if (auction.Enchantments?.Count == 2 && auction.Tag == "ENCHANTED_BOOK")
            {
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

        private static HashSet<Enchantment.EnchantmentType> GetMinLvl6(SaveAuction auction)
        {
            // may make a problem for nonrelevant 7+
            return auction.Enchantments.Where(e => e.Level >= 6 && !RelevantEnchants.ContainsKey(e.Type)).Select(e => e.Type).ToHashSet();
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
}
