using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using EnyimRedux.Caching;
using EnyimRedux.Caching.SampleWebApp.Models;
using EnyimRedux.Caching.SampleWebApp.Services;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Caching.Distributed;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace EnyimRedux.Caching.SampleWebApp
{
    public class Startup
    {
        public Startup(IConfiguration configuration)
        {
            Configuration = configuration;
        }

        public IConfiguration Configuration { get; }

        public void ConfigureServices(IServiceCollection services)
        {
            services.AddEnyimMemcached();
            services.AddEnyimMemcached<PostBody>(Configuration, "postbodyMemcached");
            //services.AddEnyimMemcached(Configuration);
            //services.AddEnyimMemcached(Configuration, "enyimMemcached");
            //services.AddEnyimMemcached(Configuration.GetSection("enyimMemcached"));
            services.AddTransient<IBlogPostService, BlogPostService>();
            services.AddMvc();
        }

        public void Configure(IApplicationBuilder app)
        {
            app.UseEnyimMemcached();

            app.UseRouting();
            app.UseEndpoints(endpoints =>
            {
                endpoints.MapDefaultControllerRoute();
            });
        }
    }
}
