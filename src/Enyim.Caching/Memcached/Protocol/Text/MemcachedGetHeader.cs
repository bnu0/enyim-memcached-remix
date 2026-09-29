namespace Enyim.Caching.Memcached.Protocol.Text
{
    internal readonly struct MemcachedGetHeader
    {
        public static MemcachedGetHeader End { get; } = new MemcachedGetHeader(null, 0, 0, 0, isEnd: true);

        public MemcachedGetHeader(string key, ushort flags, int length, ulong cas)
            : this(key, flags, length, cas, isEnd: false)
        {
        }

        private MemcachedGetHeader(string key, ushort flags, int length, ulong cas, bool isEnd)
        {
            Key = key;
            Flags = flags;
            Length = length;
            Cas = cas;
            IsEnd = isEnd;
        }

        public bool IsEnd { get; }

        public string Key { get; }

        public ushort Flags { get; }

        public int Length { get; }

        public ulong Cas { get; }
    }
}
