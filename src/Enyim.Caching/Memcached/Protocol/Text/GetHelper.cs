using System;
using System.Collections.Generic;

namespace Enyim.Caching.Memcached.Protocol.Text
{
    internal static class GetHelper
    {
        private static readonly Enyim.Caching.ILog log = Enyim.Caching.LogManager.GetLogger(typeof(GetHelper));
        internal static readonly byte[] EndToken = { (byte)'E', (byte)'N', (byte)'D' };
        internal static readonly byte[] ValueToken = { (byte)'V', (byte)'A', (byte)'L', (byte)'U', (byte)'E' };

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
            if (!TextSocketHelper.TryReadGetHeader(socket, out MemcachedGetHeader header))
            {
                throw new MemcachedClientException("Empty response received.");
            }

            if (header.IsEnd)
            {
                return ReadItemStatus.End;
            }

            byte[] allData = new byte[header.Length];
            ReadPayloadAndEndMarker(socket, allData, header.Length);

            items[header.Key] = new CacheItem(header.Flags, new ArraySegment<byte>(allData, 0, header.Length));
            cas[header.Key] = header.Cas;

            if (log.IsDebugEnabled)
            {
                log.DebugFormat("Received value. Data type: {0}, size: {1}.", header.Flags, header.Length);
            }

            return ReadItemStatus.ItemRead;
        }

        public static GetResponse ReadItem(PooledSocket socket)
        {
            if (!TextSocketHelper.TryReadGetHeader(socket, out MemcachedGetHeader header))
            {
                throw new MemcachedClientException("Empty response received.");
            }

            if (header.IsEnd)
            {
                return null;
            }

            byte[] allData = new byte[header.Length];
            ReadPayloadAndEndMarker(socket, allData, header.Length);

            GetResponse retval = new GetResponse(header.Key, header.Flags, header.Cas, allData);

            if (log.IsDebugEnabled)
            {
                log.DebugFormat("Received value. Data type: {0}, size: {1}.", retval.Item.Flags, retval.Item.Data.Count);
            }

            return retval;
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
