using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Coflnet.Sky.Core;
using Microsoft.Extensions.Configuration;
using Newtonsoft.Json;
using NUnit.Framework;
using NUnit.Framework.Legacy;

namespace Coflnet.Sky.Flipper
{
    public class FlippingEngineTest
    {
        [Test]
        public void TestMatch()
        {

            var test = FlipperEngine.ExtractRelevantEnchants(new SaveAuction()
            {
                Enchantments = new List<Enchantment>() {
                    new Enchantment(Enchantment.EnchantmentType.cleave, 4),
                    new Enchantment(Enchantment.EnchantmentType.cubism, 6),
                    new Enchantment(Enchantment.EnchantmentType.ultimate_one_for_all, 1),
                    new Enchantment(Enchantment.EnchantmentType.snipe, 4),
                    new Enchantment(Enchantment.EnchantmentType.compact, 7) }
            }).Select(e => e.Type).ToList();
            Console.WriteLine(JSON.Stringify(test));
            CollectionAssert.Contains(test, Enchantment.EnchantmentType.ultimate_one_for_all);
            CollectionAssert.Contains(test, Enchantment.EnchantmentType.cubism);
            CollectionAssert.Contains(test, Enchantment.EnchantmentType.snipe);
            CollectionAssert.DoesNotContain(test, Enchantment.EnchantmentType.cleave);
        }

        [Test]
        public void TestAddEnchantmentSubselectAbore()
        {
            var newAuction = new SaveAuction()
            {
                Uuid = "92ddfaae7e4e46b29eb2d652d9b043ac",
                Count = 1,
                StartingBid = 19000000,
                Tag = "SPEED_WITHER_LEGGINGS",
                ItemName = "Ancient Maxor's Leggings ✪✪✪✪✪",
                Start = DateTime.Now,
                End = DateTime.Now + TimeSpan.FromHours(1),
                AuctioneerId = "be87f03b822140a5a47f7f66ad59486a",
                ProfileId = "bbdd749634534a39948c791b611d080d",
                Enchantments = new List<Enchantment>()
                {
                    new Enchantment()
                    {
                        Type = Enchantment.EnchantmentType.thorns,
                        Level = 3
                    },
                    new Enchantment()
                    {
                        Type = Enchantment.EnchantmentType.rejuvenate,
                        Level = 5
                    },
                    new Enchantment()
                    {
                        Type = Enchantment.EnchantmentType.protection,
                        Level = 6
                    },
                    new Enchantment()
                    {
                        Type = Enchantment.EnchantmentType.growth,
                        Level = 5
                    },
                },
                Reforge = ItemReferences.Reforge.ancient,
                Category = Category.ARMOR,
                Tier = Tier.MYTHIC,
                Bin = true,
                FlatenedNBT = new Dictionary<string, string>() {
                    {"rarity_upgrades", "1"},
                    {"hpc", "10"},
                    {"color", "93:47:185"},
                    {"dungeon_item_level", "5"},
                    {"uid", "f5922c6f7047"}
                }
            };
            var tracking = new FindTracking();
            var result = FlipperEngine.AddEnchantmentSubselect(
                newAuction,
                FlipperEngine.ExtractRelevantEnchants(newAuction),
                GetReferenceAuctions().AsQueryable(),
                0,
                Enchantment.EnchantmentType.unknown,
                tracking);
            // only one of the auctions matches
            Assert.That("fc92b460920d486494732beeed57ed77", Is.EqualTo(result.Single().Uuid));
        }


        private List<SaveAuction> GetReferenceAuctions()
        {
            var data = System.IO.File.ReadAllText("Mock/ultireflist.json");
            Console.WriteLine(data.Truncate(30));
            return JsonConvert.DeserializeObject<List<SaveAuction>>(data);
        }

        [Test]
        public async Task GetWeightedMedian()
        {
            var low = new SaveAuction() { HighestBidAmount = 3, End = DateTime.Now - TimeSpan.FromSeconds(5) };
            var target = new SaveAuction() { HighestBidAmount = 7, End = DateTime.Now - TimeSpan.FromSeconds(6) };
            var highest = new SaveAuction() { HighestBidAmount = 11, End = DateTime.Now - TimeSpan.FromSeconds(7) };
            var newest = new SaveAuction() { HighestBidAmount = 4, End = DateTime.Now };
            var references = new List<SaveAuction>()
            {
                low,
                target,target,
                newest,
                highest,
                highest
            };
            var mockConfig = new ConfigurationBuilder().AddInMemoryCollection(new Dictionary<string, string>() { { "API_BASE_URL", "http://mock.url" } }).Build();
            var result = await new FlipperEngine(null, new Commands.Shared.GemPriceService(null, null, mockConfig), null, null, null, null, null, null,null)
                            .GetWeightedMedian(new SaveAuction(), references);
            // chooses the recent median
            Assert.That(newest.HighestBidAmount, Is.EqualTo(result));
        }

        public class NbtMock : INBT
        {
            public NBTLookup[] CreateLookup(string auctionTag, Dictionary<string, object> data, List<KeyValuePair<string, object>> flatList = null)
            {
                return new NBTLookup[]
                {
                    new NBTLookup()
                    {
                        KeyId = GetKeyId("skin"),
                        Value = GetItemIdForSkin("skin")
                    }
                };
            }

            public NBTLookup[] CreateLookup(SaveAuction auction)
            {
                throw new NotImplementedException();
            }

            public long GetItemIdForSkin(string name)
            {
                return 1;
            }

            public short GetKeyId(string name)
            {
                // hash to get short 
                return (short)(name.GetHashCode() % 10000);
            }

            public int GetValueId(short key, string value)
            {
                throw new Exception("should not be used (int value)");
            }
        }


        [Test]
        [TestCase("PET_ITEM_ALL_SKILLS_BOOST_COMMON", 30000000, false)]
        [TestCase("PET_ITEM_ALL_SKILLS_BOOST_COMMON", 3000000, true)]
        [TestCase("PET_ITEM_TIER_BOOST", 30000000, true)]
        [TestCase("PET_ITEM_TIER_BOOST", 300000, true)]
        public void PetItemSelects(string item, long exp, bool target)
        {
            Assert.That(target == FlipperEngine.ShouldPetItemMatch(new() { { "heldItem", item }, { "exp", exp.ToString() } }, 10));
        }

        [Test]
        [TestCase("[Lvl 143] Test", "[Lvl 14_] Test")]
        [TestCase("[Lvl 14] Test", "[Lvl 1_] Test")]
        [TestCase("[Lvl 1] Test", "[Lvl _] Test")]
        public void TestPetLevelComp(string full, string target)
        {
            Assert.That(target == FlipperEngine.GetPetLevelSelectVal(new() { ItemName = full }));
        }

    }
}