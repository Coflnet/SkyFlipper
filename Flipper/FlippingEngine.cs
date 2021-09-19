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
        /// <summary>
        /// List of ultimate enchantments
        /// </summary>
        public static ConcurrentDictionary<Enchantment.EnchantmentType, bool> UltimateEnchants = new ConcurrentDictionary<Enchantment.EnchantmentType, bool>();

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
            Buckets = Prometheus.Histogram.LinearBuckets(start: 20, width: 15, count: 10)
        });
        static Prometheus.HistogramConfiguration buckets = new Prometheus.HistogramConfiguration()
        {
            Buckets = Prometheus.Histogram.LinearBuckets(start: 0, width: 2, count: 10)
        };
        static Prometheus.Histogram runtroughTime = Prometheus.Metrics.CreateHistogram("sky_flipper_auction_to_send_flip_seconds", "Seconds from loading the auction to finding the flip. (should be close to 0)",
            buckets);
        static Prometheus.Histogram receiveTime = Prometheus.Metrics.CreateHistogram("sky_flipper_auction_receive_seconds", "Seconds that the flipper received an auction. (should be close to 0)",
            buckets);

        public DateTime LastLiveProbe = DateTime.Now;

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
                using (var p = new ProducerBuilder<string, FlipInstance>(producerConfig).SetValueSerializer(SerializerFactory.GetSerializer<FlipInstance>()).Build())
                {
                    c.Subscribe(NewAuctionTopic);
                    try
                    {
                        await Task.Yield();
                        Console.WriteLine("starting worker");
                        var taskFactory = new TaskFactory(new LimitedConcurrencyLevelTaskScheduler(3));
                        while (!cancleToken.IsCancellationRequested)
                        {
                            try
                            {
                                LastLiveProbe = DateTime.Now;
                                var cr = c.Consume(cancleToken);
                                if (cr == null)
                                    continue;
                                var tracer = OpenTracing.Util.GlobalTracer.Instance;
                                var parent = tracer.Extract(BuiltinFormats.TextMap, cr.Message.Value.TraceContext);
                                var span = OpenTracing.Util.GlobalTracer.Instance.BuildSpan("SearchFlip")
                                        .AsChildOf(parent);
                                var receiveSpan = OpenTracing.Util.GlobalTracer.Instance.BuildSpan("ReceiveAuction")
                                        .AsChildOf(parent).Start();
                                taskFactory.StartNew(async () =>
                                {
                                    receiveSpan.Finish();
                                    using (var scope = span.StartActive())
                                        await ProcessSingleFlip(p, cr);
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

        private async Task ProcessSingleFlip(IProducer<string, FlipInstance> p, ConsumeResult<Ignore, SaveAuction> cr)
        {
            receiveTime.Observe((DateTime.Now - cr.Message.Value.FindTime).TotalSeconds);

            using var scope = serviceFactory.CreateScope();
            var context = scope.ServiceProvider.GetRequiredService<HypixelContext>();

            var flip = await NewAuction(cr.Message.Value, context);
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

        public async System.Threading.Tasks.Task<FlipInstance> NewAuction(SaveAuction auction, HypixelContext context)
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




            var recomendedBuyUnder = medianPrice * 0.8;
            if (price > recomendedBuyUnder) // at least 20% profit
            {
                return null; // not a good flip
            }

            relevantAuctionIds[auction.UId] = relevantAuctions.Select(a => a.UId == 0 ? AuctionService.Instance.GetId(a.Uuid) : a.UId).ToList();
            if (relevantAuctionIds.Count > 10000)
            {
                relevantAuctionIds.Clear();
            }
            var itemTag = auction.Tag;
            var name = PlayerSearch.Instance.GetNameWithCacheAsync(auction.AuctioneerId);
            var filters = new Dictionary<string,string>();
            var ulti = auction.Enchantments.Where(e => UltimateEnchants.ContainsKey(e.Type)).FirstOrDefault();
            if(ulti != null)
            {
                filters["Enchantment"] = ulti.Type.ToString();
                filters["EnchantLvl"] = ulti.Level.ToString();
            }
            if(relevantReforges.Contains(auction.Reforge))
            {
                filters["Reforge"] = auction.Reforge.ToString();
            }
            filters["Rarity"] = auction.Tier.ToString();

            var exactLowestTask = ItemPrices.GetLowestBin(itemTag, auction.Tier);
            List<ItemPrices.AuctionPreview> lowestBin = await ItemPrices.GetLowestBin(itemTag, auction.Tier);
            var exactLowest = await exactLowestTask;
            if(exactLowest.Count > 1)
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

            time.Observe((DateTime.Now - auction.Start).TotalSeconds);
            return flip;
        }


        /// <summary>
        /// Gets relevant items for an auction, checks cache first
        /// </summary>
        /// <param name="auction"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public async Task<(List<SaveAuction>, DateTime)> GetRelevantAuctionsCache(SaveAuction auction, HypixelContext context)
        {
            var key = $"n{auction.ItemId}{auction.ItemName}{auction.Tier}{auction.Bin}{auction.Count}";
            key += String.Concat(auction.Enchantments.Select(a => $"{a.Type}{a.Level}"));
            key += String.Concat(auction.FlatenedNBT.Where(d => !new string[] { "uid", "spawnedFor", "bossId" }.Contains(d.Key)));
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
            var relevantEnchants = auction.Enchantments?.Where(e => UltimateEnchants.ContainsKey(e.Type) || e.Level >= 6).ToList();
            var matchingCount = relevantEnchants.Count > 2 ? relevantEnchants.Count / 2 : relevantEnchants.Count;
            var ulti = relevantEnchants.Where(e => UltimateEnchants.ContainsKey(e.Type)).FirstOrDefault();
            var highLvlEnchantList = relevantEnchants.Where(e => !UltimateEnchants.ContainsKey(e.Type)).Select(a => a.Type).ToList();
            var oldest = DateTime.Now - TimeSpan.FromHours(1);

            IQueryable<SaveAuction> select = GetSelect(auction, context, clearedName, itemId, youngest, matchingCount, ulti, highLvlEnchantList, oldest, auction.Reforge, 10);

            var relevantAuctions = await select
                .ToListAsync();

            if (relevantAuctions.Count < 9)
            {
                // to few auctions in last hour, try a whole day
                oldest = DateTime.Now - TimeSpan.FromDays(1.5);
                relevantAuctions = await GetSelect(auction, context, clearedName, itemId, youngest, matchingCount, ulti, highLvlEnchantList, oldest, auction.Reforge, 90)
                .ToListAsync();

                if (relevantAuctions.Count < 50 && PotetialFlipps.Count < 2000)
                {
                    // to few auctions in a day, query a week
                    oldest = DateTime.Now - TimeSpan.FromDays(8);
                    relevantAuctions = await GetSelect(auction, context, clearedName, itemId, youngest, matchingCount, ulti, highLvlEnchantList, oldest, auction.Reforge, 120)
                    .ToListAsync();
                    if (relevantAuctions.Count < 10 && clearedName.Contains("✪"))
                    {
                        clearedName = clearedName.Replace("✪", "").Trim();
                        relevantAuctions = await GetSelect(auction, context, clearedName, itemId, youngest, matchingCount, ulti, highLvlEnchantList, oldest, auction.Reforge, 120)
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

        // Godly on armor
        // fabled 
        // toolsmith 
        // precise
        // Renowned
        // Treacherous
        // lucky on fishing rods
        // Spiritual about a mil
        // Gilded like 12 m
        // Silky, shaded on  talisman
        // fleet, Auspicious from reforge ..
        // toil
        // fruitful, blessed, moil (on axe), warped (on aotv or aote)
        // submerged, withered, stellar (picaxe/dril), lucky, jaded (sorrow armor and divan, maybe just above some tier)
        // ambered
        private readonly static HashSet<ItemReferences.Reforge> relevantReforges = new HashSet<ItemReferences.Reforge>()
        {
            ItemReferences.Reforge.ancient,
            ItemReferences.Reforge.Necrotic,
            ItemReferences.Reforge.Giant
        };
        // include pet items lucky clover, shemlet, quick cloth, golden cloth, buble gum, text book

        private static IQueryable<SaveAuction> GetSelect(
            SaveAuction auction,
            HypixelContext context,
            string clearedName,
            int itemId,
            DateTime youngest,
            int matchingCount,
            Enchantment ulti,
            List<Enchantment.EnchantmentType> highLvlEnchantList,
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
            var val = long.Parse(flatNbt[keyValue]);
            select = select.Where(a => a.NBTLookup.Where(n => n.KeyId == keyId && n.Value == val).Any());
            return select;
        }

        private static IQueryable<SaveAuction> AddMidasSelect(IQueryable<SaveAuction> select, Dictionary<string, string> flatNbt, string keyValue)
        {
            var val = long.Parse(flatNbt[keyValue]);
            var keyId = NBT.GetLookupKey(keyValue);
            select = select.Where(a => a.NBTLookup.Where(n => n.KeyId == keyId && n.Value > val - 2_000_000 && n.Value < val + 2_000_000).Any());
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

        private static IQueryable<SaveAuction> AddEnchantmentSubselect(SaveAuction auction, int matchingCount, List<Enchantment.EnchantmentType> highLvlEnchantList, IQueryable<SaveAuction> select, byte ultiLevel, Enchantment.EnchantmentType ultiType)
        {
            var maxImportantEnchants = highLvlEnchantList.Count() + 1 + (ultiType == Enchantment.EnchantmentType.unknown ? 0 : 1);
            if (matchingCount > 0)
                select = select.Where(a => a.Enchantments
                        .Where(e => (e.Level > 5 && highLvlEnchantList.Contains(e.Type)
                                    || e.Type == ultiType && e.Level == ultiLevel)).Count() >= matchingCount
                                    && a.Enchantments.Where(e => UltiEnchantList.Contains(e.Type) || e.Level > 5).Count() <= maxImportantEnchants);
            else if (auction.Enchantments?.Count == 1)
                select = select.Where(a => a.Enchantments != null && a.Enchantments.Count() == 1
                        && a.Enchantments.First().Type == auction.Enchantments.First().Type
                        && a.Enchantments.First().Level == auction.Enchantments.First().Level);
            else if (auction.Enchantments?.Count == 2)
            {
                select = select.Where(a => a.Enchantments != null && a.Enchantments.Count() == 2
                        && a.Enchantments.Where(e =>
                            e.Type == auction.Enchantments[0].Type && e.Level == auction.Enchantments[0].Level
                            || e.Type == auction.Enchantments[1].Type && e.Level == auction.Enchantments[1].Level).Count() == 2);
            }

            // make sure we exclude special enchants to get a reasonable price
            else if (auction.Enchantments.Any())
                select = select.Where(a => !a.Enchantments.Where(e => UltiEnchantList.Contains(e.Type) || e.Level > 5).Any());
            else
                select = select.Where(a => !a.Enchantments.Any());
            return select;
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
