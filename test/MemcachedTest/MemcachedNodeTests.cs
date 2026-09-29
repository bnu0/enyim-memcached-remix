using Enyim.Caching.TestCommon;
using Enyim.Caching;
using Enyim.Caching.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;

namespace MemcachedTest
{
    public class MemcachedNodeTests
    {
        [Fact]
        public async Task ConnectionIdleTimeout_reached()
        {
            IServiceCollection services = new ServiceCollection();
            var idleTimeout = TimeSpan.FromSeconds(2);
            services.AddEnyimMemcached(options =>
            {
                options.AddServer(MemcachedTestHost.Hostname, 11211);
                options.SocketPool = new SocketPoolOptions
                {
                    ConnectionIdleTimeout = idleTimeout
                };
            });

            var originConsoleOut = Console.Out;
            using var sw = new StringWriter();
            Console.SetOut(sw);
            services.AddLogging(builder => builder.SetMinimumLevel(LogLevel.Information).AddConsole());
            IServiceProvider sp = services.BuildServiceProvider();
            var client = sp.GetRequiredService<IMemcachedClient>() as MemcachedClient;

            var logMessage = $"Connection idle timeout {idleTimeout} reached";
            await client.GetAsync(Guid.NewGuid().ToString());

            await Task.Delay(2100);
            await client.GetAsync(Guid.NewGuid().ToString());
            Assert.Contains(logMessage, sw.ToString());

            Console.SetOut(originConsoleOut);
        }

        [Fact]
        public async Task ConcurrentGets_WithSmallPool_DoNotExceedMaxPoolSize()
        {
            IServiceCollection services = new ServiceCollection();
            services.AddEnyimMemcached(options =>
            {
                options.AddServer(MemcachedTestHost.Hostname, 11211);
                options.SocketPool = new SocketPoolOptions
                {
                    MinPoolSize = 0,
                    MaxPoolSize = 4,
                    QueueTimeout = TimeSpan.FromSeconds(5)
                };
            });
            services.AddLogging(builder => builder.SetMinimumLevel(LogLevel.Warning));
            IServiceProvider sp = services.BuildServiceProvider();
            var client = sp.GetRequiredService<IMemcachedClient>();

            var key = Guid.NewGuid().ToString();
            Assert.True(await client.SetAsync(key, "v", 60));

            var tasks = Enumerable.Range(0, 64).Select(_ => client.GetAsync<string>(key));
            var results = await Task.WhenAll(tasks);

            Assert.All(results, r =>
            {
                Assert.True(r.Success);
                Assert.Equal("v", r.Value);
            });
        }
    }
}
