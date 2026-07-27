using System;
using System.Buffers;
using System.Buffers.Text;
using System.Text;

namespace EnyimRedux.Caching.Memcached.Protocol.Text
{
    internal readonly struct MemcachedResponseLine
    {
        public MemcachedResponseLine(byte[] buffer, int length, int partCount, int part0Start, int part0Length, int part1Start, int part1Length, int part2Start, int part2Length, int part3Start, int part3Length, int part4Start, int part4Length)
        {
            Buffer = buffer;
            Length = length;
            PartCount = partCount;
            Part0Start = part0Start;
            Part0Length = part0Length;
            Part1Start = part1Start;
            Part1Length = part1Length;
            Part2Start = part2Start;
            Part2Length = part2Length;
            Part3Start = part3Start;
            Part3Length = part3Length;
            Part4Start = part4Start;
            Part4Length = part4Length;
        }

        public byte[] Buffer { get; }

        public int Length { get; }

        public int PartCount { get; }

        public int Part0Start { get; }
        public int Part0Length { get; }
        public int Part1Start { get; }
        public int Part1Length { get; }
        public int Part2Start { get; }
        public int Part2Length { get; }
        public int Part3Start { get; }
        public int Part3Length { get; }
        public int Part4Start { get; }
        public int Part4Length { get; }

        public ReadOnlySpan<byte> GetPart(int index)
        {
            switch (index)
            {
                case 0: return Buffer.AsSpan(Part0Start, Part0Length);
                case 1: return Buffer.AsSpan(Part1Start, Part1Length);
                case 2: return Buffer.AsSpan(Part2Start, Part2Length);
                case 3: return Buffer.AsSpan(Part3Start, Part3Length);
                case 4: return Buffer.AsSpan(Part4Start, Part4Length);
                default: throw new ArgumentOutOfRangeException(nameof(index));
            }
        }

        public bool PartEquals(int index, ReadOnlySpan<byte> value)
        {
            return GetPart(index).SequenceEqual(value);
        }

        public static bool TryParseUInt16(ReadOnlySpan<byte> value, out ushort result)
        {
            return Utf8Parser.TryParse(value, out result, out _);
        }

        public static bool TryParseInt32(ReadOnlySpan<byte> value, out int result)
        {
            return Utf8Parser.TryParse(value, out result, out _);
        }

        public static bool TryParseUInt64(ReadOnlySpan<byte> value, out ulong result)
        {
            return Utf8Parser.TryParse(value, out result, out _);
        }

        public static string GetAsciiString(ReadOnlySpan<byte> value)
        {
#if NET8_0_OR_GREATER
            return Encoding.ASCII.GetString(value);
#else
            return Encoding.ASCII.GetString(value.ToArray());
#endif
        }
    }
}
