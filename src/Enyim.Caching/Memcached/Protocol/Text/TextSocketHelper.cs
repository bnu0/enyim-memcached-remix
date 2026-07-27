using System;
using System.Buffers;
using System.IO;
using System.Text;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Extensions.ObjectPool;

namespace EnyimRedux.Caching.Memcached.Protocol.Text
{
    internal static class TextSocketHelper
    {
        private const string GenericErrorResponse = "ERROR";
        private static byte[] GenericErrorResponseBytes = Encoding.ASCII.GetBytes(GenericErrorResponse);
        private const string ClientErrorResponse = "CLIENT_ERROR ";
        private static byte[] ClientErrorResponseBytes = Encoding.ASCII.GetBytes(ClientErrorResponse);
        private const string ServerErrorResponse = "SERVER_ERROR ";
        private static byte[] ServerErrorResponseBytes = Encoding.ASCII.GetBytes(ServerErrorResponse);
        private const int ErrorResponseLength = 13;

        public const string CommandTerminator = "\r\n";

        private static readonly EnyimRedux.Caching.ILog log = EnyimRedux.Caching.LogManager.GetLogger(typeof(TextSocketHelper));

        private static readonly ObjectPool<SegmentedMemoryStream> SegmentedMemoryStreamPool =
            new DefaultObjectPoolProvider { MaximumRetained = 64 }
                .Create(new SegmentedMemoryStreamPoolPolicy());

        private sealed class SegmentedMemoryStreamPoolPolicy : IPooledObjectPolicy<SegmentedMemoryStream>
        {
            public SegmentedMemoryStream Create() => new SegmentedMemoryStream(1024);

            public bool Return(SegmentedMemoryStream obj)
            {
                obj.Reset();
                return true;
            }
        }

        /// <summary>
        /// Reads the response of the server.
        /// </summary>
        /// <returns>The data sent by the memcached server.</returns>
        /// <exception cref="T:System.InvalidOperationException">The server did not sent a response or an empty line was returned.</exception>
        /// <exception cref="T:EnyimRedux.Caching.Memcached.MemcachedException">The server did not specified any reason just returned the string ERROR. - or - The server returned a SERVER_ERROR, in this case the Message of the exception is the message returned by the server.</exception>
        /// <exception cref="T:EnyimRedux.Caching.Memcached.MemcachedClientException">The server did not recognize the request sent by the client. The Message of the exception is the message returned by the server.</exception>
        public static string ReadResponse(PooledSocket socket)
        {
            string response = TextSocketHelper.ReadLine(socket);
            if (response == null)
            {
                return string.Empty;
            }

            if (log.IsDebugEnabled)
                log.Debug("Received response: " + response);

            if (String.IsNullOrEmpty(response))
                throw new MemcachedClientException("Empty response received.");

            if (String.Compare(response, GenericErrorResponse, StringComparison.Ordinal) == 0)
                throw new NotSupportedException("Operation is not supported by the server or the request was malformed. If the latter please report the bug to the developers.");

            if (response.Length >= ErrorResponseLength)
            {
                if (String.Compare(response, 0, ClientErrorResponse, 0, ErrorResponseLength, StringComparison.Ordinal) == 0)
                {
                    throw new MemcachedClientException(response.Remove(0, ErrorResponseLength));
                }
                else if (String.Compare(response, 0, ServerErrorResponse, 0, ErrorResponseLength, StringComparison.Ordinal) == 0)
                {
                    throw new MemcachedException(response.Remove(0, ErrorResponseLength));
                }
            }

            return response;
        }
        
        public static (string[], int) ReadResponseParts(PooledSocket socket)
        {
            if (!TryReadResponseLine(socket, out var line))
            {
                return (Array.Empty<string>(), 0);
            }

            try
            {
                if (log.IsDebugEnabled)
                {
                    log.Debug("Received response: " + MemcachedResponseLine.GetAsciiString(line.Buffer.AsSpan(0, line.Length)));
                }

                ValidateResponseLine(line);

                var ret = new string[line.PartCount];
                for (int i = 0; i < line.PartCount; i++)
                {
                    ret[i] = MemcachedResponseLine.GetAsciiString(line.GetPart(i));
                }

                return (ret, line.PartCount);
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(line.Buffer);
            }
        }

        internal static bool TryReadResponseLine(PooledSocket socket, out MemcachedResponseLine line)
        {
            line = default;
            var ms = ReadSocketContet(socket);
            if (ms == null)
            {
                return false;
            }

            var buff = ArrayPool<byte>.Shared.Rent(ms.Length);
            try
            {
                ms.ToArray(buff);
                if (!TryParseResponseLine(buff, ms.Length, out line))
                {
                    ArrayPool<byte>.Shared.Return(buff);
                    return false;
                }

                return true;
            }
            finally
            {
                SegmentedMemoryStreamPool.Return(ms);
            }
        }

        private static void ValidateResponseLine(in MemcachedResponseLine line)
        {
            if (line.Length == 0)
            {
                throw new MemcachedClientException("Empty response received.");
            }

            if (line.Length >= GenericErrorResponseBytes.Length
                && line.Buffer.AsSpan(0, GenericErrorResponseBytes.Length).SequenceEqual(GenericErrorResponseBytes))
            {
                throw new NotSupportedException(
                    "Operation is not supported by the server or the request was malformed. If the latter please report the bug to the developers.");
            }

            if (line.Length >= ErrorResponseLength)
            {
                if (line.Buffer.AsSpan(0, ClientErrorResponseBytes.Length).SequenceEqual(ClientErrorResponseBytes))
                {
                    throw new MemcachedClientException(MemcachedResponseLine.GetAsciiString(
                        line.Buffer.AsSpan(ErrorResponseLength, line.Length - ErrorResponseLength)));
                }

                if (line.Buffer.AsSpan(0, ServerErrorResponseBytes.Length).SequenceEqual(ServerErrorResponseBytes))
                {
                    throw new MemcachedException(MemcachedResponseLine.GetAsciiString(
                        line.Buffer.AsSpan(0, ErrorResponseLength)));
                }
            }
        }

        private static bool TryParseResponseLine(byte[] buffer, int length, out MemcachedResponseLine line)
        {
            int partCount = 0;
            int part0Start = 0, part0Length = 0;
            int part1Start = 0, part1Length = 0;
            int part2Start = 0, part2Length = 0;
            int part3Start = 0, part3Length = 0;
            int part4Start = 0, part4Length = 0;
            int lastIndex = 0;

            for (int i = 0; i < length; i++)
            {
                if (partCount >= 5)
                {
                    throw new MemcachedException("Found too many parts\r\n" + Encoding.ASCII.GetString(buffer, 0, length));
                }

                if (buffer[i] == 0x20)
                {
                    AssignPart(partCount, lastIndex, i - lastIndex, ref part0Start, ref part0Length, ref part1Start, ref part1Length, ref part2Start, ref part2Length, ref part3Start, ref part3Length, ref part4Start, ref part4Length);
                    partCount++;
                    lastIndex = i + 1;
                }
            }

            if (lastIndex < length)
            {
                if (partCount >= 5)
                {
                    throw new MemcachedException("Found too many parts\r\n" + Encoding.ASCII.GetString(buffer, 0, length));
                }

                AssignPart(partCount, lastIndex, length - lastIndex, ref part0Start, ref part0Length, ref part1Start, ref part1Length, ref part2Start, ref part2Length, ref part3Start, ref part3Length, ref part4Start, ref part4Length);
                partCount++;
            }

            line = new MemcachedResponseLine(
                buffer,
                length,
                partCount,
                part0Start,
                part0Length,
                part1Start,
                part1Length,
                part2Start,
                part2Length,
                part3Start,
                part3Length,
                part4Start,
                part4Length);
            return true;
        }

        private static void AssignPart(
            int partCount,
            int start,
            int length,
            ref int part0Start,
            ref int part0Length,
            ref int part1Start,
            ref int part1Length,
            ref int part2Start,
            ref int part2Length,
            ref int part3Start,
            ref int part3Length,
            ref int part4Start,
            ref int part4Length)
        {
            switch (partCount)
            {
                case 0:
                    part0Start = start;
                    part0Length = length;
                    break;
                case 1:
                    part1Start = start;
                    part1Length = length;
                    break;
                case 2:
                    part2Start = start;
                    part2Length = length;
                    break;
                case 3:
                    part3Start = start;
                    part3Length = length;
                    break;
                case 4:
                    part4Start = start;
                    part4Length = length;
                    break;
            }
        }

        /// <summary>
        /// Reads a line from the socket. A line is terninated by \r\n.
        /// </summary>
        /// <returns></returns>
        private static string ReadLine(PooledSocket socket)
        {
            var ms = ReadSocketContet(socket);
            if (ms == null)
            {
                return string.Empty;
            }

            try
            {
                string retval = ms.ConvertToAscii();
                if (log.IsDebugEnabled)
                {
                    log.Debug("ReadLine: " + retval);
                }

                return retval;
            }
            finally
            {
                SegmentedMemoryStreamPool.Return(ms);
            }
        }

        private static SegmentedMemoryStream ReadSocketContet(PooledSocket socket)
        {
            var ms = SegmentedMemoryStreamPool.Get();

            bool gotR = false;

            int data;

            while (true)
            {
                data = socket.ReadByte();

                if (data == -1) // EOF / half-open → kill this socket
                {
                    log.Warn("Socket EOF/half-open, killing socket");
                    socket.IsAlive = false;
                    SegmentedMemoryStreamPool.Return(ms);
                    return null;
                }

                if (data == 13)
                {
                    gotR = true;
                    continue;
                }

                if (gotR)
                {
                    if (data == 10)
                        break;

                    ms.WriteByte(13);

                    gotR = false;
                }

                ms.WriteByte((byte) data);
            }

            return ms;
        }

#if NET8_0_OR_GREATER
        internal static string CreateCommand(string content)
        {
            int totalLength = content.Length + CommandTerminator.Length;

            return string.Create(totalLength, content, static (span, value) =>
            {
                value.AsSpan().CopyTo(span);
                CommandTerminator.AsSpan().CopyTo(span.Slice(value.Length));
            });
        }

        internal static string CreateCommand(string prefix, string suffix)
        {
            int totalLength = prefix.Length + suffix.Length + CommandTerminator.Length;

            return string.Create(totalLength, (prefix, suffix), static (span, parts) =>
            {
                int position = 0;
                parts.prefix.AsSpan().CopyTo(span.Slice(position));
                position += parts.prefix.Length;
                parts.suffix.AsSpan().CopyTo(span.Slice(position));
                position += parts.suffix.Length;
                CommandTerminator.AsSpan().CopyTo(span.Slice(position));
            });
        }

        internal static string CreateCommand(string prefix, string part1, string separator, string part2)
        {
            int totalLength = prefix.Length + part1.Length + separator.Length + part2.Length + CommandTerminator.Length;

            return string.Create(totalLength, (prefix, part1, separator, part2), static (span, parts) =>
            {
                int position = 0;
                parts.prefix.AsSpan().CopyTo(span.Slice(position));
                position += parts.prefix.Length;
                parts.part1.AsSpan().CopyTo(span.Slice(position));
                position += parts.part1.Length;
                parts.separator.AsSpan().CopyTo(span.Slice(position));
                position += parts.separator.Length;
                parts.part2.AsSpan().CopyTo(span.Slice(position));
                position += parts.part2.Length;
                CommandTerminator.AsSpan().CopyTo(span.Slice(position));
            });
        }

        internal static string CreateMultiGetCommand(string commandPrefix, IList<string> keys)
        {
            int totalLength = commandPrefix.Length + CommandTerminator.Length;
            if (keys.Count > 0)
            {
                totalLength += keys.Sum(k => k.Length) + keys.Count - 1;
            }

            return string.Create(totalLength, (commandPrefix, keys), static (span, state) =>
            {
                int position = 0;
                state.commandPrefix.AsSpan().CopyTo(span.Slice(position));
                position += state.commandPrefix.Length;
                for (int i = 0; i < state.keys.Count; i++)
                {
                    if (i > 0)
                    {
                        span[position++] = ' ';
                    }

                    state.keys[i].CopyTo(span.Slice(position));
                    position += state.keys[i].Length;
                }

                CommandTerminator.AsSpan().CopyTo(span.Slice(position));
            });
        }

        internal static string CreateStoreCommand(
            string commandPrefix,
            string key,
            string flags,
            string expires,
            string dataLength,
            string cas = null)
        {
            int totalLength = commandPrefix.Length + key.Length + 1
                + flags.Length + 1 + expires.Length + 1 + dataLength.Length
                + (cas == null ? 0 : 1 + cas.Length)
                + CommandTerminator.Length;

            return string.Create(totalLength, (commandPrefix, key, flags, expires, dataLength, cas), static (span, parts) =>
            {
                int position = 0;
                position = AppendCommandPart(span, position, parts.commandPrefix);
                position = AppendCommandPart(span, position, parts.key);
                span[position++] = ' ';
                position = AppendCommandPart(span, position, parts.flags);
                span[position++] = ' ';
                position = AppendCommandPart(span, position, parts.expires);
                span[position++] = ' ';
                position = AppendCommandPart(span, position, parts.dataLength);
                if (parts.cas != null)
                {
                    span[position++] = ' ';
                    position = AppendCommandPart(span, position, parts.cas);
                }

                CommandTerminator.AsSpan().CopyTo(span.Slice(position));
            });
        }

        private static int AppendCommandPart(Span<char> span, int position, string value)
        {
            value.AsSpan().CopyTo(span.Slice(position));
            return position + value.Length;
        }
#endif

        /// <summary>
        /// Gets the bytes representing the specified command. returned buffer can be used to streamline multiple writes into one Write on the Socket
        /// using the <see cref="M:EnyimRedux.Caching.Memcached.PooledSocket.Write(IList&lt;ArraySegment&lt;byte&gt;&gt;)"/>
        /// </summary>
        /// <param name="value">The command to be converted.</param>
        /// <returns>The buffer containing the bytes representing the command. The command must be terminated by \r\n.</returns>
        /// <remarks>The Nagle algorithm is disabled on the socket to speed things up, so it's recommended to convert a command into a buffer
        /// and use the <see cref="M:EnyimRedux.Caching.Memcached.PooledSocket.Write(IList&lt;ArraySegment&lt;byte&gt;&gt;)"/> to send the command and the additional buffers in one transaction.</remarks>
        public unsafe static IList<ArraySegment<byte>> GetCommandBuffer(string value)
        {
            return TextCommandBuffer.FromString(value);
        }

        public unsafe static IList<ArraySegment<byte>> GetCommandBuffer(string value, IList<ArraySegment<byte>> list)
        {
            return TextCommandBuffer.FromString(value, list);
        }

#if NET8_0_OR_GREATER
        internal static IList<ArraySegment<byte>> GetCommandBufferPrefixSuffix(ReadOnlySpan<char> prefix, ReadOnlySpan<char> suffix)
        {
            return TextCommandBuffer.FromPrefixSuffix(prefix, suffix);
        }

        internal static IList<ArraySegment<byte>> GetCommandBufferPrefixPartSeparatorPart(
            ReadOnlySpan<char> prefix,
            ReadOnlySpan<char> part1,
            ReadOnlySpan<char> separator,
            ReadOnlySpan<char> part2)
        {
            return TextCommandBuffer.FromPrefixPartSeparatorPart(prefix, part1, separator, part2);
        }

        internal static IList<ArraySegment<byte>> GetCommandBufferMultiGet(ReadOnlySpan<char> commandPrefix, IList<string> keys)
        {
            return TextCommandBuffer.FromMultiGet(commandPrefix, keys);
        }
#endif

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
