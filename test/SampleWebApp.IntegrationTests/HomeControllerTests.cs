using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;
using EnyimRedux.Caching;
using EnyimRedux.Caching.SampleWebApp;
using EnyimRedux.Caching.SampleWebApp.Controllers;
using EnyimRedux.Caching.SampleWebApp.Models;
using EnyimRedux.Caching.TestCommon;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace SampleWebApp.IntegrationTests
{
    public class HomeControllerTests : IClassFixture<WebApplicationFactory<Startup>>
    {
        private readonly WebApplicationFactory<Startup> _factory;

        public HomeControllerTests(WebApplicationFactory<Startup> factory)
        {
            _factory = factory.WithWebHostBuilder(builder =>
            {
                builder.ConfigureAppConfiguration((_, config) =>
                {
                    config.AddInMemoryCollection(new Dictionary<string, string>
                    {
                        ["enyimMemcached:Servers:0:Address"] = MemcachedTestHost.Hostname,
                        ["postbodyMemcached:Servers:0:Address"] = MemcachedTestHost.Hostname
                    });
                });
            });
        }

        [Fact]
        public async Task HomeController_Index()
        {
            var httpClient = _factory.CreateClient();
            var response = await httpClient.GetAsync("/");
            Assert.Equal(HttpStatusCode.OK, response.StatusCode);

            var memcachedClient = _factory.Server.Host.Services.GetRequiredService<IMemcachedClient>();
            var postsDict = await memcachedClient.GetValueAsync<Dictionary<string, List<BlogPost>>>(HomeController.CacheKey);
            Assert.NotNull(postsDict);
            Assert.NotEmpty(postsDict.First().Value.First().Title);

            await memcachedClient.RemoveAsync(HomeController.CacheKey);
        }

        [Fact]
        public async Task Get_postbody_from_cache_ok()
        {
            var httpClient = _factory.CreateClient();
            var response = await httpClient.GetAsync("/home/postbody");
            Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        }
    }
}
