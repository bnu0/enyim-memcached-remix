using System;
using System.Net;
using System.Net.Sockets;

namespace Enyim.Caching.TestCommon
{
    public static class MemcachedTestHost
    {
        private static readonly Lazy<string> Host = new Lazy<string>(Resolve);

        public static string Hostname => Host.Value;

        private static string Resolve()
        {
            var envHost = Environment.GetEnvironmentVariable("MEMCACHED_HOST");
            if (!string.IsNullOrEmpty(envHost))
            {
                return envHost;
            }

            try
            {
                Dns.GetHostEntry("memcached");
                return "memcached";
            }
            catch (SocketException)
            {
                return "127.0.0.1";
            }
        }
    }
}
