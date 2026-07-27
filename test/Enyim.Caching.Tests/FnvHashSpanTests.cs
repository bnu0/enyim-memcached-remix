using System;
using System.Collections.Generic;
using System.Text;
using Xunit;

namespace EnyimRedux.Caching.Tests
{
    public class FnvHashSpanTests
    {
        private static readonly List<Tuple<string, uint>> TestVectors = new()
        {
            new("", 0x811c9dc5U),
            new("a", 0xe40c292cU),
            new("b", 0xe70c2de5U),
            new("c", 0xe60c2c52U),
            new("d", 0xe10c2473U),
            new("e", 0xe00c22e0U),
            new("f", 0xe30c2799U),
            new("fo", 0x6222e842U),
            new("foo", 0xa9f37ed7U),
            new("foob", 0x3f5076efU),
        };

        [Fact]
        public void Hash_MatchesComputeHashForByteSpans()
        {
            var fnv = new FNV1a(true);

            foreach (var testVector in TestVectors)
            {
                byte[] data = Encoding.ASCII.GetBytes(testVector.Item1);
                uint expected = BitConverter.ToUInt32(fnv.ComputeHash(data), 0);
                uint actual = FNV1a.Hash(data.AsSpan());

                Assert.Equal(expected, actual);
                Assert.Equal(testVector.Item2, actual);
            }
        }

        [Fact]
        public void HashAscii_MatchesHashForAsciiBytes()
        {
            foreach (var testVector in TestVectors)
            {
                byte[] data = Encoding.ASCII.GetBytes(testVector.Item1);
                uint fromBytes = FNV1a.Hash(data.AsSpan());
                uint fromChars = FNV1a.HashAscii(testVector.Item1.AsSpan());

                Assert.Equal(fromBytes, fromChars);
                Assert.Equal(testVector.Item2, fromChars);
            }
        }

        [Fact]
        public void HashAscii_MatchesComputeHashForLocatorKeys()
        {
            var fnv = new FNV1a(true);
            var keys = new[]
            {
                "consistent-hash-key",
                "Hello_Multi_Get_0",
                Guid.NewGuid().ToString(),
                "mykey",
            };

            foreach (var key in keys)
            {
                byte[] data = Encoding.ASCII.GetBytes(key);
                uint expected = BitConverter.ToUInt32(fnv.ComputeHash(data), 0);
                Assert.Equal(expected, FNV1a.HashAscii(key.AsSpan()));
            }
        }
    }
}
