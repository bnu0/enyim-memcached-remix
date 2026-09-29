using Enyim.Caching.Memcached;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Xunit;

namespace Enyim.Caching.Tests
{
    public class TextProtocolIntegrationTests : MemcachedClientTestsBase
    {
        public TextProtocolIntegrationTests()
            : base(options => options.Protocol = MemcachedProtocol.Text)
        {
        }

        private void StoreForTextProtocol(string key, object value)
        {
            Assert.True(_client.Store(StoreMode.Set, key, value, TimeSpan.FromMinutes(1)));
        }

        [Fact]
        public void TextGet_ParsesValueResponseForStoredItem()
        {
            var key = GetUniqueKey("text_get");
            var value = GetRandomString();
            StoreForTextProtocol(key, value);

            var getResult = _client.ExecuteGet(key);
            GetAssertPass(getResult, value);
        }

        [Fact]
        public void TextMultiGet_ParsesMultipleValueLines()
        {
            var keys = GetUniqueKeys(max: 8).ToList();
            foreach (var key in keys)
            {
                StoreForTextProtocol(key, "value-" + key);
            }

            var results = _client.ExecuteGet(keys);
            Assert.Equal(keys.Count, results.Count);

            foreach (var key in keys)
            {
                Assert.True(results[key].Success, "Get failed for key: " + key);
                Assert.Equal("value-" + key, results[key].Value);
            }
        }

        [Fact]
        public void TextMultiGetWithCas_ParsesFivePartValueLines()
        {
            var keys = GetUniqueKeys(max: 6).ToList();
            foreach (var key in keys)
            {
                StoreForTextProtocol(key, key);
            }

            var results = _client.GetWithCas(keys);
            Assert.Equal(keys.Count, results.Count);

            foreach (var key in keys)
            {
                Assert.True(results.ContainsKey(key));
                Assert.Equal(key, results[key].Result);
                Assert.True(results[key].Cas > 0);
            }
        }

        [Fact]
        public void TextGetBinaryPayload_ParsesLengthFieldCorrectly()
        {
            var key = GetUniqueKey("text_binary");
            var payload = Enumerable.Range(0, 512).Select(i => (byte)(i % 256)).ToArray();

            StoreForTextProtocol(key, payload);
            var getResult = _client.ExecuteGet(key);

            GetAssertPass(getResult, payload);
        }

        [Fact]
        public async Task TextMultiGetManyKeys_PreallocatedResultDictionaryWorks()
        {
            const int keyCount = 20;
            var keys = GetUniqueKeys(max: keyCount).ToList();

            foreach (var key in keys)
            {
                Assert.True(await StoreAsync(key: key, value: key));
            }

            var results = await _client.GetAsync<string>(keys);
            Assert.Equal(keyCount, results.Count);

            foreach (var key in keys)
            {
                Assert.Equal(key, results[key]);
            }
        }
    }
}
