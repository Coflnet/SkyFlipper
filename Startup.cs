using Coflnet.Sky.Commands.Shared;
using Coflnet.Sky.Core;
using Coflnet.Sky.Items.Client.Api;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.OpenApi.Models;
using Prometheus;

namespace Coflnet.Sky.Flipper
{
    public class Startup
    {
        public Startup(IConfiguration configuration)
        {
            Configuration = configuration;
        }

        public IConfiguration Configuration { get; }

        // This method gets called by the runtime. Use this method to add services to the container.
        public void ConfigureServices(IServiceCollection services)
        {

            services.AddControllers();
            services.AddSwaggerGen(c =>
            {
                c.SwaggerDoc("v1", new OpenApiInfo { Title = "SkyFlipper", Version = "v1" });
            });
            services.AddJaeger();

            services.AddDbContext<HypixelContext>(
                dbContextOptions => dbContextOptions
                    .UseMySql(Configuration["DBCONNECTION"], new MariaDbServerVersion(Configuration["MARIADB_VERSION"]))
                    .EnableSensitiveDataLogging() // <-- These two calls are optional but help
                    .EnableDetailedErrors()       // <-- with debugging (remove for production).
            );


            services.AddSingleton<IItemsApi>(context =>
            {
                var config = context.GetRequiredService<IConfiguration>();
                return new ItemsApi(config["ITEMS_BASE_URL"]);
            });

            services.AddHostedService<FlipperService>();
            services.AddHostedService<AuctionCheckService>();

            services.AddSingleton<GemPriceService>();
            services.AddSingleton<FlipperEngine>();
            services.AddHostedService<GemPriceService>(di => di.GetRequiredService<GemPriceService>());
            NBT.Instance = new NoWriteNbt();
        }

        public class NoWriteNbt : NBT
        {
            protected override NBTValue AddNewValueToDb((short, string) k, HypixelContext context)
            {
                return new NBTValue();
            }
        }

        // This method gets called by the runtime. Use this method to configure the HTTP request pipeline.
        public void Configure(IApplicationBuilder app, IWebHostEnvironment env)
        {
            if (env.IsDevelopment())
            {
                app.UseDeveloperExceptionPage();
            }
            app.UseSwagger();
            app.UseSwaggerUI(c => c.SwaggerEndpoint("/swagger/v1/swagger.json", "SkyFlipper v1"));

            app.UseRouting();

            app.UseAuthorization();

            app.UseEndpoints(endpoints =>
            {
                endpoints.MapMetrics();
                endpoints.MapControllers();
            });
        }
    }
}
