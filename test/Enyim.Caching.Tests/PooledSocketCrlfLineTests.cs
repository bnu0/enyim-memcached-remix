using Enyim.Caching.Memcached;
using Microsoft.Extensions.Logging.Abstractions;
using System;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading.Tasks;
using Xunit;

namespace Enyim.Caching.Tests
{
    public class PooledSocketCrlfLineTests
    {
        [Fact]
        public void TryReadCrlfLine_ReadsValueHeaderFullyInFirstFill()
        {
            using var pair = ConnectedPair.Start();
            pair.WriteAscii("VALUE k 0 1 1\r\n");

            var dest = new byte[512];
            Assert.True(pair.Client.TryReadCrlfLine(dest, out int n, out bool truncated));
            Assert.False(truncated);
            Assert.Equal("VALUE k 0 1 1", Encoding.ASCII.GetString(dest, 0, n));
        }

        [Fact]
        public void TryReadCrlfLine_ReadsEnd()
        {
            using var pair = ConnectedPair.Start();
            pair.WriteAscii("END\r\n");

            var dest = new byte[512];
            Assert.True(pair.Client.TryReadCrlfLine(dest, out int n, out bool truncated));
            Assert.False(truncated);
            Assert.Equal("END", Encoding.ASCII.GetString(dest, 0, n));
        }

        [Fact]
        public async Task TryReadCrlfLine_HeaderSplitAcrossSocketFills()
        {
            using var pair = ConnectedPair.Start();
            pair.WriteAscii("VALUE k");
            pair.Flush();

            var dest = new byte[512];
            var read = Task.Run(() =>
            {
                Assert.True(pair.Client.TryReadCrlfLine(dest, out int n, out bool truncated));
                return (n, truncated);
            });

            await Task.Delay(50);
            pair.WriteAscii(" 0 5 99\r\n");
            var (n, truncated) = await read;

            Assert.False(truncated);
            Assert.Equal("VALUE k 0 5 99", Encoding.ASCII.GetString(dest, 0, n));
        }

        [Fact]
        public async Task TryReadCrlfLine_CrAtEndOfEightKilobyteFill()
        {
            using var pair = ConnectedPair.Start();
            var prefix = new byte[8191];
            for (int i = 0; i < prefix.Length; i++)
            {
                prefix[i] = (byte)'x';
            }

            pair.WriteBytes(prefix);
            pair.WriteBytes(new byte[] { (byte)'\r' });
            pair.Flush();

            var dest = new byte[16384];
            var read = Task.Run(() =>
            {
                Assert.True(pair.Client.TryReadCrlfLine(dest, out int n, out bool truncated));
                return (n, truncated);
            });

            await Task.Delay(50);
            pair.WriteBytes(new byte[] { (byte)'\n' });
            var (n, truncated) = await read;

            Assert.False(truncated);
            Assert.Equal(8191, n);
            Assert.Equal((byte)'x', dest[0]);
            Assert.Equal((byte)'x', dest[8190]);
        }

        [Fact]
        public void TryReadCrlfLine_EofMarksSocketDead()
        {
            using var pair = ConnectedPair.Start();
            pair.CloseServer();

            var dest = new byte[512];
            Assert.False(pair.Client.TryReadCrlfLine(dest, out _, out _));
            Assert.False(pair.Client.IsAlive);
        }

        [Fact]
        public void TryReadCrlfLine_TruncatedLineContinuesIntoLargerBuffer()
        {
            using var pair = ConnectedPair.Start();
            pair.WriteAscii("abcdefghij\r\n");

            var first = new byte[4];
            Assert.True(pair.Client.TryReadCrlfLine(first, out int n1, out bool truncated1));
            Assert.True(truncated1);
            Assert.Equal(4, n1);
            Assert.Equal("abcd", Encoding.ASCII.GetString(first, 0, n1));

            var rest = new byte[16];
            Assert.True(pair.Client.TryReadCrlfLine(rest, out int n2, out bool truncated2));
            Assert.False(truncated2);
            Assert.Equal("efghij", Encoding.ASCII.GetString(rest, 0, n2));
        }

        [Fact]
        public void TryReadCrlfLine_FivePartGetsHeader()
        {
            using var pair = ConnectedPair.Start();
            pair.WriteAscii("VALUE hashed-key 12 8 9876543210\r\n");

            var dest = new byte[512];
            Assert.True(pair.Client.TryReadCrlfLine(dest, out int n, out bool truncated));
            Assert.False(truncated);
            Assert.Equal("VALUE hashed-key 12 8 9876543210", Encoding.ASCII.GetString(dest, 0, n));
        }

        private sealed class ConnectedPair : IDisposable
        {
            private readonly TcpListener _listener;
            private readonly TcpClient _serverClient;
            private readonly NetworkStream _serverStream;

            public PooledSocket Client { get; }

            private ConnectedPair(TcpListener listener, TcpClient serverClient, PooledSocket client)
            {
                _listener = listener;
                _serverClient = serverClient;
                _serverStream = serverClient.GetStream();
                Client = client;
            }

            public static ConnectedPair Start()
            {
                var listener = new TcpListener(IPAddress.Loopback, 0);
                listener.Start();
                var port = ((IPEndPoint)listener.LocalEndpoint).Port;
                var accept = listener.AcceptTcpClientAsync();
                var client = new PooledSocket(
                    new IPEndPoint(IPAddress.Loopback, port),
                    TimeSpan.FromSeconds(3),
                    TimeSpan.FromSeconds(3),
                    NullLogger.Instance,
                    useSslStream: false,
                    useIPv6: false);
                client.Connect();
                var serverClient = accept.GetAwaiter().GetResult();
                return new ConnectedPair(listener, serverClient, client);
            }

            public void WriteAscii(string text)
            {
                WriteBytes(Encoding.ASCII.GetBytes(text));
            }

            public void WriteBytes(byte[] data)
            {
                _serverStream.Write(data, 0, data.Length);
                _serverStream.Flush();
            }

            public void Flush()
            {
                _serverStream.Flush();
            }

            public void CloseServer()
            {
                try { _serverStream.Close(); } catch { }
                try { _serverClient.Close(); } catch { }
            }

            public void Dispose()
            {
                try { Client.Destroy(); } catch { }
                try { _serverStream.Dispose(); } catch { }
                try { _serverClient.Dispose(); } catch { }
                try { _listener.Stop(); } catch { }
            }
        }
    }
}
