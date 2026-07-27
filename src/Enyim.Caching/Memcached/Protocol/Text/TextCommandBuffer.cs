using System;
using System.Buffers;
using System.Buffers.Text;
using System.Collections.Generic;
using System.Globalization;
using System.Text;

namespace EnyimRedux.Caching.Memcached.Protocol.Text
{
    internal static class TextCommandBuffer
    {
        private const byte Cr = (byte)'\r';
        private const byte Lf = (byte)'\n';
        private const byte Space = (byte)' ';

        internal static IList<ArraySegment<byte>> FromPrefixSuffix(ReadOnlySpan<char> prefix, ReadOnlySpan<char> suffix)
        {
            var length = prefix.Length + suffix.Length + 2;
            var bytes = AllocateCommandBytes(length);
            WritePrefixSuffix(bytes, prefix, suffix);
            return new[] { new ArraySegment<byte>(bytes, 0, length) };
        }

        internal static IList<ArraySegment<byte>> FromPrefixPartSeparatorPart(
            ReadOnlySpan<char> prefix,
            ReadOnlySpan<char> part1,
            ReadOnlySpan<char> separator,
            ReadOnlySpan<char> part2)
        {
            var length = prefix.Length + part1.Length + separator.Length + part2.Length + 2;
            var bytes = AllocateCommandBytes(length);
            var pos = 0;
            pos = WriteChars(bytes, pos, prefix);
            pos = WriteChars(bytes, pos, part1);
            pos = WriteChars(bytes, pos, separator);
            pos = WriteChars(bytes, pos, part2);
            bytes[pos++] = Cr;
            bytes[pos] = Lf;
            return new[] { new ArraySegment<byte>(bytes, 0, length) };
        }

        internal static IList<ArraySegment<byte>> FromMultiGet(ReadOnlySpan<char> commandPrefix, IList<string> keys)
        {
            var length = commandPrefix.Length + 2;
            if (keys.Count > 0)
            {
                length += keys.Count - 1;
                for (int i = 0; i < keys.Count; i++)
                {
                    length += keys[i].Length;
                }
            }

            var bytes = AllocateCommandBytes(length);
            var pos = WriteChars(bytes, 0, commandPrefix);
            for (int i = 0; i < keys.Count; i++)
            {
                if (i > 0)
                {
                    bytes[pos++] = Space;
                }

                pos = WriteChars(bytes, pos, keys[i].AsSpan());
            }

            bytes[pos++] = Cr;
            bytes[pos] = Lf;
            return new[] { new ArraySegment<byte>(bytes, 0, length) };
        }

        internal static void AppendStoreHeader(
            IList<ArraySegment<byte>> buffers,
            ReadOnlySpan<char> commandPrefix,
            ReadOnlySpan<char> key,
            uint flags,
            uint expires,
            int dataLength,
            ulong cas,
            bool includeCas)
        {
            Span<byte> flagsBuf = stackalloc byte[16];
            Span<byte> expiresBuf = stackalloc byte[16];
            Span<byte> lengthBuf = stackalloc byte[16];
            Span<byte> casBuf = stackalloc byte[24];

            if (!Utf8Formatter.TryFormat(flags, flagsBuf, out int flagsLen))
            {
                throw new MemcachedClientException("Failed to format store flags.");
            }

            if (!Utf8Formatter.TryFormat(expires, expiresBuf, out int expiresLen))
            {
                throw new MemcachedClientException("Failed to format store expires.");
            }

            if (!Utf8Formatter.TryFormat(dataLength, lengthBuf, out int lengthLen))
            {
                throw new MemcachedClientException("Failed to format store data length.");
            }

            int casLen = 0;
            if (includeCas)
            {
                if (!Utf8Formatter.TryFormat(cas, casBuf, out casLen))
                {
                    throw new MemcachedClientException("Failed to format store cas.");
                }
            }

            var headerLength = commandPrefix.Length + key.Length + 1
                + flagsLen + 1 + expiresLen + 1 + lengthLen
                + (includeCas ? 1 + casLen : 0)
                + 2;

            var bytes = AllocateCommandBytes(headerLength);
            var pos = WriteChars(bytes, 0, commandPrefix);
            pos = WriteChars(bytes, pos, key);
            bytes[pos++] = Space;
            flagsBuf.Slice(0, flagsLen).CopyTo(bytes.AsSpan(pos));
            pos += flagsLen;
            bytes[pos++] = Space;
            expiresBuf.Slice(0, expiresLen).CopyTo(bytes.AsSpan(pos));
            pos += expiresLen;
            bytes[pos++] = Space;
            lengthBuf.Slice(0, lengthLen).CopyTo(bytes.AsSpan(pos));
            pos += lengthLen;
            if (includeCas)
            {
                bytes[pos++] = Space;
                casBuf.Slice(0, casLen).CopyTo(bytes.AsSpan(pos));
                pos += casLen;
            }

            bytes[pos++] = Cr;
            bytes[pos] = Lf;
            buffers.Add(new ArraySegment<byte>(bytes, 0, headerLength));
        }

        internal static IList<ArraySegment<byte>> FromString(string value)
        {
            return FromChars(value.AsSpan());
        }

        internal static IList<ArraySegment<byte>> FromString(string value, IList<ArraySegment<byte>> list)
        {
            list.Add(FromChars(value.AsSpan())[0]);
            return list;
        }

        private static IList<ArraySegment<byte>> FromChars(ReadOnlySpan<char> chars)
        {
            var bytes = AllocateCommandBytes(chars.Length);
            WriteChars(bytes, 0, chars);
            return new[] { new ArraySegment<byte>(bytes, 0, chars.Length) };
        }

        private static void WritePrefixSuffix(byte[] bytes, ReadOnlySpan<char> prefix, ReadOnlySpan<char> suffix)
        {
            var pos = WriteChars(bytes, 0, prefix);
            pos = WriteChars(bytes, pos, suffix);
            bytes[pos++] = Cr;
            bytes[pos] = Lf;
        }

        private static int WriteChars(byte[] bytes, int offset, ReadOnlySpan<char> chars)
        {
            for (int i = 0; i < chars.Length; i++)
            {
                bytes[offset++] = (byte)chars[i];
            }

            return offset;
        }

        private static byte[] AllocateCommandBytes(int length)
        {
#if NET8_0_OR_GREATER
            return GC.AllocateUninitializedArray<byte>(length);
#else
            return new byte[length];
#endif
        }
    }
}
