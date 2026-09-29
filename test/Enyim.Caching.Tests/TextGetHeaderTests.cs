using Enyim.Caching.Configuration;
using Enyim.Caching.Memcached;
using Enyim.Caching.Memcached.Results;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace Enyim.Caching.Tests
{
    public class TextGetHeaderTests
    {
        [Fact]
        public void TextMultiGet_ParsesValuePayloadAndEnd()
        {
            using var server = FakeMemcached.Start();
            using var client = CreateTextClient(server.Port);

            var op = (IMultiGetOperation)client.ServerPool.OperationFactory.MultiGet(new[] { "k1" });
            var result = client.ServerPool.GetWorkingNodes().First().Execute(op);

            Assert.True(result.Success, result.Message + " " + result.Exception);
            Assert.True(op.Result.ContainsKey("k1"));
            var data = op.Result["k1"].Data;
            Assert.Equal("hello", Encoding.ASCII.GetString(data.Array, data.Offset, data.Count));
            Assert.Equal(1UL, op.Cas["k1"]);
        }

        [Fact]
        public void TextMultiGet_EofBeforeLine_FailsWithEmptyResponseReceived()
        {
            using var server = FakeMemcached.Start(closeOnGets: true);
            using var client = CreateTextClient(server.Port);

            var result = ExecuteMultiGet(client, new[] { "k1" });

            Assert.False(result.Success);
            Assert.Contains("Empty response received", result.Message, StringComparison.OrdinalIgnoreCase);
        }

        [Fact]
        public void TextMultiGet_LongServerError_DoesNotUseStreamPool()
        {
            using var server = FakeMemcached.Start(serverError: new string('e', 800));
            using var client = CreateTextClient(server.Port);

            var result = ExecuteMultiGet(client, new[] { "k1" });

            Assert.False(result.Success);
            Assert.Contains("No VALUE response received", result.Message + result.Exception, StringComparison.Ordinal);
        }

        private static IOperationResult ExecuteMultiGet(ProbeClient client, IList<string> keys)
        {
            var node = client.ServerPool.GetWorkingNodes().First();
            return node.Execute(client.ServerPool.OperationFactory.MultiGet(keys));
        }

        private static ProbeClient CreateTextClient(int port)
        {
            var loggerFactory = LoggerFactory.Create(builder => builder.SetMinimumLevel(LogLevel.Warning));
            var options = new MemcachedClientOptions
            {
                Protocol = MemcachedProtocol.Text,
                SuppressException = true,
                SocketPool = new SocketPoolOptions
                {
                    MinPoolSize = 0,
                    MaxPoolSize = 2,
                    ConnectionTimeout = TimeSpan.FromSeconds(3),
                    ReceiveTimeout = TimeSpan.FromSeconds(3),
                    QueueTimeout = TimeSpan.FromSeconds(5),
                    ConnectionIdleTimeout = TimeSpan.Zero,
                    FailurePolicyFactory = new ThrottlingFailurePolicyFactory(int.MaxValue, TimeSpan.FromHours(1))
                }
            };
            options.AddServer("127.0.0.1", port);
            var config = new MemcachedClientConfiguration(loggerFactory, Options.Create(options));
            return new ProbeClient(loggerFactory, config);
        }

        private sealed class ProbeClient : MemcachedClient
        {
            public ProbeClient(ILoggerFactory loggerFactory, IMemcachedClientConfiguration configuration)
                : base(loggerFactory, configuration)
            {
            }

            public IServerPool ServerPool => Pool;
        }

        private sealed class FakeMemcached : IDisposable
        {
            private readonly TcpListener _listener;
            private readonly CancellationTokenSource _cts = new();
            private readonly Task _acceptLoop;

            public int Port { get; }

            private FakeMemcached(TcpListener listener, bool closeOnGets, string serverError)
            {
                _listener = listener;
                Port = ((IPEndPoint)listener.LocalEndpoint).Port;
                _acceptLoop = Task.Run(() => AcceptLoop(closeOnGets, serverError));
            }

            public static FakeMemcached Start(bool closeOnGets = false, string serverError = null)
            {
                var listener = new TcpListener(IPAddress.Loopback, 0);
                listener.Start();
                return new FakeMemcached(listener, closeOnGets, serverError);
            }

            private async Task AcceptLoop(bool closeOnGets, string serverError)
            {
                try
                {
                    while (!_cts.IsCancellationRequested)
                    {
                        var client = await _listener.AcceptTcpClientAsync(_cts.Token).ConfigureAwait(false);
                        _ = Task.Run(() => HandleClient(client, closeOnGets, serverError), _cts.Token);
                    }
                }
                catch (OperationCanceledException)
                {
                }
                catch (ObjectDisposedException)
                {
                }
            }

            private static void HandleClient(TcpClient client, bool closeOnGets, string serverError)
            {
                using (client)
                using (var stream = client.GetStream())
                {
                    var buffer = new byte[8192];
                    var pending = new MemoryStream();

                    while (true)
                    {
                        int read;
                        try
                        {
                            read = stream.Read(buffer, 0, buffer.Length);
                        }
                        catch
                        {
                            return;
                        }

                        if (read <= 0)
                        {
                            return;
                        }

                        pending.Write(buffer, 0, read);
                        if (!TryConsume(pending, stream, closeOnGets, serverError))
                        {
                            return;
                        }
                    }
                }
            }

            private static bool TryConsume(MemoryStream pending, NetworkStream stream, bool closeOnGets, string serverError)
            {
                while (true)
                {
                    var data = pending.ToArray();
                    var lineEnd = IndexOfCrlf(data);
                    if (lineEnd < 0)
                    {
                        return true;
                    }

                    var line = Encoding.ASCII.GetString(data, 0, lineEnd);
                    var remainderStart = lineEnd + 2;
                    ReplacePending(pending, data, remainderStart);

                    if (line.StartsWith("gets ", StringComparison.Ordinal) || line.StartsWith("get ", StringComparison.Ordinal))
                    {
                        if (closeOnGets)
                        {
                            try { stream.Close(); } catch { }
                            return false;
                        }

                        if (serverError != null)
                        {
                            WriteAscii(stream, "SERVER_ERROR " + serverError + "\r\n");
                            continue;
                        }

                        WriteAscii(stream, "VALUE k1 0 5 1\r\nhello\r\nEND\r\n");
                        continue;
                    }
                }
            }

            private static void ReplacePending(MemoryStream pending, byte[] data, int consumed)
            {
                pending.SetLength(0);
                if (consumed < data.Length)
                {
                    pending.Write(data, consumed, data.Length - consumed);
                }
            }

            private static int IndexOfCrlf(byte[] data)
            {
                for (int i = 0; i < data.Length - 1; i++)
                {
                    if (data[i] == (byte)'\r' && data[i + 1] == (byte)'\n')
                    {
                        return i;
                    }
                }

                return -1;
            }

            private static void WriteAscii(NetworkStream stream, string text)
            {
                var bytes = Encoding.ASCII.GetBytes(text);
                stream.Write(bytes, 0, bytes.Length);
                stream.Flush();
            }

            public void Dispose()
            {
                _cts.Cancel();
                try { _listener.Stop(); } catch { }
                try { _acceptLoop.Wait(TimeSpan.FromSeconds(1)); } catch { }
                _cts.Dispose();
            }
        }
    }
}
