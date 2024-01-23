using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Coflnet.Sky.Core;
using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;
using OpenTracing.Util;

namespace Coflnet.Sky.Flipper.Controllers
{
    [ApiController]
    [Route("[controller]")]
    public class FlippingController : ControllerBase
    {
        private readonly ILogger<FlippingController> _logger;
        private readonly HypixelContext dbContext;
        private readonly FlipperEngine flipperEngine;

        public FlippingController(ILogger<FlippingController> logger,
        HypixelContext context,
        FlipperEngine flipperEngine)
        {
            _logger = logger;
            dbContext = context;
            this.flipperEngine = flipperEngine;
        }

        [HttpGet]
        [Route("/flip/{uuid}/based")]
        public async Task<IEnumerable<SaveAuction>> Get(string uuid)
        {
            var auction = await AuctionService.Instance.GetAuctionAsync(uuid,
                auctions => auctions
                .Include(a => a.NbtData)
                .Include(a => a.NBTLookup)
                .Include(a => a.Enchantments));
            _logger.LogInformation(Newtonsoft.Json.JsonConvert.SerializeObject(auction));
            if (auction == null)
                return new List<SaveAuction>();

            return (await flipperEngine.GetRelevantAuctionsCache(auction, new FindTracking())).references;

        }
        [HttpGet]
        [Route("/flip/{uuid}/cache")]
        public async Task<object> GetCacheInfo(string uuid)
        {
            var auction = await AuctionService.Instance.GetAuctionAsync(uuid,
                auctions => auctions
                .Include(a => a.NbtData)
                .Include(a => a.NBTLookup)
                .Include(a => a.Enchantments));
            if (auction == null)
                return new List<SaveAuction>();
            var cached = await flipperEngine.GetRelevantAuctionsCache(auction, new FindTracking());
            var estimate = flipperEngine.GetWeightedMedian(auction, cached.references);
            return new { cached.HitCount, cached.Key, cached.QueryTime, refCount = cached.references.Count(), estimate };

        }

        [HttpDelete]
        [Route("/flip/{uuid}")]
        public async Task<IActionResult> Delete(string uuid)
        {
            var auction = await AuctionService.Instance.GetAuctionAsync(uuid,
                auctions => auctions
                .Include(a => a.NbtData)
                .Include(a => a.NBTLookup)
                .Include(a => a.Enchantments));
            if (auction == null)
                return NotFound();
            await flipperEngine.VoidReferences(auction);
            return Ok();
        }

        [HttpGet]
        [Route("/status")]
        public IActionResult Status(string uuid)
        {
            if (flipperEngine.LastLiveProbe < DateTime.Now - TimeSpan.FromMinutes(2.5))
                return StatusCode(500);
            return Ok();
        }
    }
}
