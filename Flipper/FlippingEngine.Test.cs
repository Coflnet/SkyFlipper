using System;
using System.Collections.Generic;
using System.Linq;
using hypixel;
using Newtonsoft.Json;
using NUnit.Framework;

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
                    new Enchantment(Enchantment.EnchantmentType.compact, 8) }
            }).Select(e => e.Type).ToList();
            Console.WriteLine(JSON.Stringify(test));
            CollectionAssert.Contains(test, Enchantment.EnchantmentType.ultimate_one_for_all);
            CollectionAssert.Contains(test, Enchantment.EnchantmentType.cubism);
            CollectionAssert.Contains(test, Enchantment.EnchantmentType.snipe);
            CollectionAssert.DoesNotContain(test, Enchantment.EnchantmentType.compact);
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
            Assert.AreEqual("fc92b460920d486494732beeed57ed77",result.Single().Uuid);
        }


        private List<SaveAuction> GetReferenceAuctions()
        {
            var data = System.IO.File.ReadAllText("Mock/ultireflist.json");
            Console.WriteLine(data.Truncate(30));
            return JsonConvert.DeserializeObject<List<SaveAuction>>(data);
        }
    }
}
