using System.Collections.Generic;
using System.Runtime.Serialization;
using hypixel;
using Microsoft.EntityFrameworkCore;

namespace Coflnet.Sky.Flipper
{
    /// <summary>
    /// Producer representation of flips
    /// </summary>
    [DataContract]
    public class FlipInstance
    {
        [DataMember(Name = "median")]
        public int MedianPrice;
        [DataMember(Name = "cost")]
        public int LastKnownCost;
        [DataMember(Name = "uuid")]
        public string Uuid;
        [DataMember(Name = "name")]
        public string Name;
        [DataMember(Name = "sellerName")]
        public string SellerName;
        [DataMember(Name = "volume")]
        public float Volume;
        [DataMember(Name = "tag")]
        public string Tag;
        [DataMember(Name = "bin")]
        public bool Bin;
        [DataMember(Name = "sold")]
        public bool Sold { get; set; }
        [DataMember(Name = "tier")]
        public Tier Rarity { get; set; }
        [DataMember(Name = "prop")]
        public List<string> Interesting { get; set; }
        [DataMember(Name = "secondLowestBin")]
        public long? SecondLowestBin { get; set; }

        [DataMember(Name = "lowestBin")]
        public long? LowestBin;
        [DataMember(Name = "auction")]
        public SaveAuction Auction;
        [DataMember(Name = "uId")]
        public long UId;
    }
}
