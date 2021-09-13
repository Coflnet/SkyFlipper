using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using hypixel;
using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;

namespace Coflnet.Sky.Flipper.Controllers
{
    [ApiController]
    [Route("[controller]")]
    public class FlippingController : ControllerBase
    {
        private readonly ILogger<FlippingController> _logger;
        private readonly HypixelContext dbContext;

        public FlippingController(ILogger<FlippingController> logger,
        HypixelContext context)
        {
            _logger = logger;
            dbContext = context;
        }

        [HttpGet]
        [Route("{uuid}/based")]
        public async Task<IEnumerable<SaveAuction>> Get(string uuid)
        {
            var auction = AuctionService.Instance.GetAuction(uuid,
                auctions => auctions
                .Include(a => a.NbtData)
                .Include(a => a.Enchantments));
            using (var context = new HypixelContext())
            {
                return (await FlipperEngine.Instance.GetRelevantAuctionsCache(auction, dbContext)).Item1;
            }
        }
    }
}
