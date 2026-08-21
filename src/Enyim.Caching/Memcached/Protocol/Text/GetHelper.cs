using System;
using System.Buffers;
using System.Collections.Generic;

namespace Enyim.Caching.Memcached.Protocol.Text
{
    internal static class GetHelper
    {
        private static readonly Enyim.Caching.ILog log = Enyim.Caching.LogManager.GetLogger(typeof(GetHelper));
        private static readonly byte[] EndToken = { (byte)'E', (byte)'N', (byte)'D' };
        private static readonly byte[] ValueToken = { (byte)'V', (byte)'A', (byte)'L', (byte)'U', (byte)'E' };

        internal enum ReadItemStatus
        {
            End,
            ItemRead,
        }

        public static void FinishCurrent(PooledSocket socket)
        {
            string response = TextSocketHelper.ReadResponse(socket);

            if (String.Compare(response, "END", StringComparison.Ordinal) != 0)
            {
                throw new MemcachedClientException("No END was received.");
            }
        }

        internal static void ReadItemsInto(
            PooledSocket socket,
            IDictionary<string, CacheItem> items,
            IDictionary<string, ulong> cas)
        {
            ReadItemStatus status;
            while ((status = ReadItemInto(socket, items, cas)) == ReadItemStatus.ItemRead)
            {
            }
        }

        internal static ReadItemStatus ReadItemInto(
            PooledSocket socket,
            IDictionary<string, CacheItem> items,
            IDictionary<string, ulong> cas)
        {
            if (!TextSocketHelper.TryReadResponseLine(socket, out MemcachedResponseLine line))
            {
                // Match remix ReadResponse: EOF is "Empty response received."
                throw new MemcachedClientException("Empty response received.");
            }

            try
            {
                if (line.PartCount == 1 && line.GetPart(0).SequenceEqual(EndToken))
                {
                    return ReadItemStatus.End;
                }

                if (line.PartCount < 4 || !line.GetPart(0).SequenceEqual(ValueToken))
                {
                    throw new MemcachedClientException(
                        "No VALUE response received.\r\n" +
                        MemcachedResponseLine.GetAsciiString(line.Buffer.AsSpan(0, line.Length)));
                }

                ulong casValue = 0;
                if (line.PartCount == 5)
                {
                    if (!MemcachedResponseLine.TryParseUInt64(line.GetPart(4), out casValue))
                    {
                        throw new MemcachedClientException("Invalid CAS VALUE received.");
                    }
                }

                if (!MemcachedResponseLine.TryParseUInt16(line.GetPart(2), out ushort flags))
                {
                    throw new MemcachedClientException("Invalid flags VALUE received.");
                }

                if (!MemcachedResponseLine.TryParseInt32(line.GetPart(3), out int length))
                {
                    throw new MemcachedClientException("Invalid length VALUE received.");
                }

                string key = MemcachedResponseLine.GetAsciiString(line.GetPart(1));
                byte[] allData = new byte[length];

                ReadPayloadAndEndMarker(socket, allData, length);

                items[key] = new CacheItem(flags, new ArraySegment<byte>(allData, 0, length));
                cas[key] = casValue;

                if (log.IsDebugEnabled)
                {
                    log.DebugFormat("Received value. Data type: {0}, size: {1}.", flags, length);
                }

                return ReadItemStatus.ItemRead;
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(line.Buffer);
            }
        }

        public static GetResponse ReadItem(PooledSocket socket)
        {
            if (!TextSocketHelper.TryReadResponseLine(socket, out MemcachedResponseLine line))
            {
                throw new MemcachedClientException("Empty response received.");
            }

            try
            {
                if (line.PartCount == 1 && line.GetPart(0).SequenceEqual(EndToken))
                {
                    return null;
                }

                if (line.PartCount < 4 || !line.GetPart(0).SequenceEqual(ValueToken))
                {
                    throw new MemcachedClientException(
                        "No VALUE response received.\r\n" +
                        MemcachedResponseLine.GetAsciiString(line.Buffer.AsSpan(0, line.Length)));
                }

                ulong casValue = 0;
                if (line.PartCount == 5)
                {
                    if (!MemcachedResponseLine.TryParseUInt64(line.GetPart(4), out casValue))
                    {
                        throw new MemcachedClientException("Invalid CAS VALUE received.");
                    }
                }

                if (!MemcachedResponseLine.TryParseUInt16(line.GetPart(2), out ushort flags))
                {
                    throw new MemcachedClientException("Invalid flags VALUE received.");
                }

                if (!MemcachedResponseLine.TryParseInt32(line.GetPart(3), out int length))
                {
                    throw new MemcachedClientException("Invalid length VALUE received.");
                }

                string key = MemcachedResponseLine.GetAsciiString(line.GetPart(1));
                byte[] allData = new byte[length];

                ReadPayloadAndEndMarker(socket, allData, length);

                GetResponse retval = new GetResponse(key, flags, casValue, allData);

                if (log.IsDebugEnabled)
                {
                    log.DebugFormat("Received value. Data type: {0}, size: {1}.", retval.Item.Flags, retval.Item.Data.Count);
                }

                return retval;
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(line.Buffer);
            }
        }

        private static void ReadPayloadAndEndMarker(PooledSocket socket, byte[] payload, int length)
        {
            socket.Read(payload, 0, length);

            var eod = new byte[2];
            socket.Read(eod, 0, 2);

            if (eod[0] != 13 || eod[1] != 10)
            {
                throw new MemcachedClientException("Invalid end marker after memcached value block.");
            }
        }
    }

    #region [ T:GetResponse                  ]
    public class GetResponse
    {
        private GetResponse() { }
        public GetResponse(string key, ushort flags, ulong casValue, byte[] data) : this(key, flags, casValue, data, 0, data.Length) { }

        public GetResponse(string key, ushort flags, ulong casValue, byte[] data, int offset, int count)
        {
            Key = key;
            CasValue = casValue;

            Item = new CacheItem(flags, new ArraySegment<byte>(data, offset, count));
        }

        public readonly string Key;
        public readonly ulong CasValue;
        public readonly CacheItem Item;
    }
    #endregion

}

#region [ License information          ]
/* ************************************************************
 * 
 *    Copyright (c) 2010 Attila Kiskó, enyim.com
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
