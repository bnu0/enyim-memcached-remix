using EnyimRedux.Caching;
using EnyimRedux.Caching.Memcached;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Xunit;

namespace MemcachedTest
{
    public class TextMemcachedClientTest : MemcachedClientTest
    {
        protected override MemcachedClient GetClient(MemcachedProtocol protocol = MemcachedProtocol.Text)
        {
            return base.GetClient(MemcachedProtocol.Text);
        }

        [Fact]
        public void IncrementTest()
        {
            using (MemcachedClient client = GetClient())
            {
                var key = "text_increment_" + Guid.NewGuid();
                Assert.True(client.Store(StoreMode.Set, key, "100"), "Initialization failed");

                Assert.Equal((ulong)102, client.Increment(key, 0, 2));
                Assert.Equal((ulong)112, client.Increment(key, 0, 10));
            }
        }

        [Fact]
        public void DecrementTest()
        {
            using (MemcachedClient client = GetClient())
            {
                var key = "text_decrement_" + Guid.NewGuid();
                client.Store(StoreMode.Set, key, "100");

                Assert.Equal((ulong)98, client.Decrement(key, 0, 2));
                Assert.Equal((ulong)88, client.Decrement(key, 0, 10));
            }
        }

        [Fact]
        public void CASTest()
        {
            using (MemcachedClient client = GetClient())
            {
                // store the item
                var r1 = client.Store(StoreMode.Set, "CasItem1", "foo");

                Assert.True(r1, "Initial set failed.");

                // get back the item and check the cas value (it should match the cas from the set)
                var r2 = client.GetWithCas<string>("CasItem1");

                Assert.Equal("foo", r2.Result);
                Assert.NotEqual((ulong)0, r2.Cas);

                var r3 = client.Cas(StoreMode.Set, "CasItem1", "bar", r2.Cas - 1);

                Assert.False(r3.Result, "Overwriting with 'bar' should have failed.");

                var r4 = client.Cas(StoreMode.Set, "CasItem1", "baz", r2.Cas);

                Assert.True(r4.Result, "Overwriting with 'baz' should have succeeded.");

                var r5 = client.GetWithCas<string>("CasItem1");
                Assert.Equal("baz", r5.Result);
            }
        }


        [Fact]
        public void StoreWithTimeSpan()
        {
            using (MemcachedClient client = GetClient())
            {
                var key = "abc";
                var value = "core memcache write";
                bool success = client.Store(EnyimRedux.Caching.Memcached.StoreMode.Set, key, value, new TimeSpan(0, 10, 0));
                Assert.True(success);
                Assert.Equal(value, client.Get<string>(key));
            }
        }

        [Fact]
        public async Task TextMultiGetTest()
        {
            using (var client = GetClient())
            {
                var keys = new List<string>();

                for (int i = 0; i < 10; i++)
                {
                    string k = $"text_multi_get_{Guid.NewGuid()}_{i}";
                    keys.Add(k);
                    Assert.True(await client.StoreAsync(StoreMode.Set, k, i, DateTime.Now.AddSeconds(30)), "Store of " + k + " failed");
                }

                IDictionary<string, int> results = await client.GetAsync<int>(keys);
                Assert.Equal(keys.Count, results.Count);

                for (int i = 0; i < keys.Count; i++)
                {
                    Assert.True(results.TryGetValue(keys[i], out int value), "missing key: " + keys[i]);
                    Assert.Equal(i, value);
                }
            }
        }

        [Fact]
        public async Task TextMultiGetWithCasTest()
        {
            using (var client = GetClient())
            {
                var keys = new List<string>();
                for (int i = 0; i < 10; i++)
                {
                    string k = $"text_multi_get_cas_{Guid.NewGuid()}_{i}";
                    keys.Add(k);
                    Assert.True(await client.StoreAsync(StoreMode.Set, k, i, DateTime.Now.AddSeconds(300)));
                }

                var results = await client.GetWithCasAsync(keys);
                Assert.Equal(keys.Count, results.Count);

                foreach (var key in keys)
                {
                    Assert.True(results.TryGetValue(key, out var entry));
                    Assert.NotEqual((ulong)0, entry.Cas);
                }
            }
        }

        [Fact]
        public async Task TextMultiGetManyKeysTest()
        {
            using (var client = GetClient())
            {
                const int keyCount = 25;
                var keys = new List<string>(keyCount);
                var expected = new Dictionary<string, string>(keyCount);

                for (int i = 0; i < keyCount; i++)
                {
                    string key = $"text_bulk_get_{Guid.NewGuid()}_{i}";
                    string value = $"payload-{i}-{Guid.NewGuid()}";
                    keys.Add(key);
                    expected[key] = value;
                    Assert.True(await client.StoreAsync(StoreMode.Set, key, value, DateTime.Now.AddSeconds(60)));
                }

                var results = await client.GetAsync<string>(keys);
                Assert.Equal(keyCount, results.Count);

                foreach (var pair in expected)
                {
                    Assert.Equal(pair.Value, results[pair.Key]);
                }
            }
        }

        [Fact]
        public void TextGetBinaryPayloadTest()
        {
            using (var client = GetClient())
            {
                var key = $"text_binary_{Guid.NewGuid()}";
                var payload = Enumerable.Range(0, 256).Select(i => (byte)i).ToArray();

                Assert.True(client.Store(StoreMode.Set, key, payload, TimeSpan.FromMinutes(1)));
                var roundTrip = client.Get<byte[]>(key);

                Assert.Equal(payload, roundTrip);
            }
        }
    }
}

#region [ License information          ]
/* ************************************************************
 * 
 *    Copyright (c) 2010 Attila Kisk? enyim.com
 *    
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *    
 *        http://www.apache.org/licenses/LICENSE-2.0
 *    
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 *    
 * ************************************************************/
#endregion
