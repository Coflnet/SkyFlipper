using System;
using System.Collections.Generic;
using System.Linq;
using hypixel;
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
            CollectionAssert.Contains(test,Enchantment.EnchantmentType.ultimate_one_for_all);
            CollectionAssert.Contains(test,Enchantment.EnchantmentType.cubism);
            CollectionAssert.Contains(test,Enchantment.EnchantmentType.snipe);
            CollectionAssert.Contains(test,Enchantment.EnchantmentType.compact);
            CollectionAssert.DoesNotContain(test,Enchantment.EnchantmentType.cleave);
        }
    }
}
