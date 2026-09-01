using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;

namespace Enyim.Caching.Memcached
{
    /// <summary>
    /// This is a ketama-like consistent hashing based node locator. Used when no other <see cref="T:IMemcachedNodeLocator"/> is specified for the pool.
    /// </summary>
    public sealed class DefaultNodeLocator : IMemcachedNodeLocator, IDisposable
    {
        private sealed class LocatorSnapshot
        {
            public static readonly LocatorSnapshot Empty = new(
                Array.Empty<uint>(),
                new Dictionary<uint, IMemcachedNode>(new UIntEqualityComparer()),
                Array.Empty<IMemcachedNode>(),
                new HashSet<IMemcachedNode>());

            public LocatorSnapshot(
                uint[] keys,
                Dictionary<uint, IMemcachedNode> servers,
                IReadOnlyList<IMemcachedNode> allServers,
                HashSet<IMemcachedNode> deadServers)
            {
                Keys = keys;
                Servers = servers;
                AllServers = allServers;
                DeadServers = deadServers;
            }

            public uint[] Keys { get; }

            public Dictionary<uint, IMemcachedNode> Servers { get; }

            public IReadOnlyList<IMemcachedNode> AllServers { get; }

            public HashSet<IMemcachedNode> DeadServers { get; }
        }

        private readonly int _serverAddressMutations;
        private readonly object _deadServerLock = new();
        private LocatorSnapshot _snapshot = LocatorSnapshot.Empty;

        public DefaultNodeLocator() : this(100)
        {
        }

        public DefaultNodeLocator(int serverAddressMutations)
        {
            _serverAddressMutations = serverAddressMutations;
        }

        private LocatorSnapshot BuildSnapshot(
            IReadOnlyList<IMemcachedNode> allServers,
            HashSet<IMemcachedNode> deadServers,
            IList<IMemcachedNode> ringNodes)
        {
            var servers = new Dictionary<uint, IMemcachedNode>(new UIntEqualityComparer());
            var keys = new uint[ringNodes.Count * _serverAddressMutations];

            int nodeIdx = 0;

            foreach (IMemcachedNode node in ringNodes)
            {
                var tmpKeys = GenerateKeys(node, _serverAddressMutations);

                for (var i = 0; i < tmpKeys.Length; i++)
                {
                    servers[tmpKeys[i]] = node;
                }

                tmpKeys.CopyTo(keys, nodeIdx);
                nodeIdx += _serverAddressMutations;
            }

            Array.Sort(keys);
            return new LocatorSnapshot(keys, servers, allServers, deadServers);
        }

        void IMemcachedNodeLocator.Initialize(IList<IMemcachedNode> nodes)
        {
            lock (_deadServerLock)
            {
                var snapshot = Volatile.Read(ref _snapshot) ?? LocatorSnapshot.Empty;
                var deadServers = new HashSet<IMemcachedNode>(snapshot.DeadServers);
                var allServers = nodes.ToList();
                Interlocked.Exchange(ref _snapshot, BuildSnapshot(allServers, deadServers, allServers));
            }
        }

        IMemcachedNode IMemcachedNodeLocator.Locate(string key)
        {
            if (key == null)
            {
                throw new ArgumentNullException("key");
            }

            return Locate(key);
        }

        IEnumerable<IMemcachedNode> IMemcachedNodeLocator.GetWorkingNodes()
        {
            var snapshot = Volatile.Read(ref _snapshot);
            if (snapshot == null)
            {
                return Array.Empty<IMemcachedNode>();
            }

            return snapshot.AllServers.Where(n => !snapshot.DeadServers.Contains(n)).ToArray();
        }

        private IMemcachedNode Locate(string key)
        {
            var snapshot = Volatile.Read(ref _snapshot);
            if (snapshot == null)
            {
                return null;
            }

            var node = FindNode(key, snapshot);
            if (node == null || node.IsAlive)
            {
                return node;
            }

            lock (_deadServerLock)
            {
                snapshot = Volatile.Read(ref _snapshot);
                if (snapshot == null)
                {
                    return null;
                }

                // check if it's still dead or it came back while waiting for the lock
                if (!node.IsAlive && !snapshot.DeadServers.Contains(node))
                {
                    var deadServers = new HashSet<IMemcachedNode>(snapshot.DeadServers) { node };
                    var ringNodes = snapshot.AllServers.Where(n => !deadServers.Contains(n)).ToList();
                    Interlocked.Exchange(ref _snapshot, BuildSnapshot(snapshot.AllServers, deadServers, ringNodes));
                }
            }

            // try again with the dead server removed from the lists
            return Locate(key);
        }

        /// <summary>
        /// locates a node by its key
        /// </summary>
        /// <param name="key"></param>
        /// <returns></returns>
        private static IMemcachedNode FindNode(string key, LocatorSnapshot snapshot)
        {
            if (snapshot.Keys.Length == 0)
            {
                return null;
            }

            uint itemKeyHash = FNV1a.HashAscii(key.AsSpan());

            // get the index of the server assigned to this hash
            int foundIndex = Array.BinarySearch(snapshot.Keys, itemKeyHash);

            // no exact match
            if (foundIndex < 0)
            {
                // this is the nearest server in the list
                foundIndex = ~foundIndex;

                if (foundIndex == 0)
                {
                    // it's smaller than everything, so use the last server (with the highest key)
                    foundIndex = snapshot.Keys.Length - 1;
                }
                else if (foundIndex >= snapshot.Keys.Length)
                {
                    // the key was larger than all server keys, so return the first server
                    foundIndex = 0;
                }
            }

            if (foundIndex < 0 || foundIndex > snapshot.Keys.Length)
            {
                return null;
            }

            return snapshot.Servers[snapshot.Keys[foundIndex]];
        }

        private static uint[] GenerateKeys(IMemcachedNode node, int numberOfKeys)
        {
            const int KeyLength = 4;
            const int PartCount = 1; // (ModifiedFNV.HashSize / 8) / KeyLength; // HashSize is in bits, uint is 4 byte long

            var k = new uint[PartCount * numberOfKeys];

            // every server is registered numberOfKeys times
            // using UInt32s generated from the different parts of the hash
            // i.e. hash is 64 bit:
            // 00 00 aa bb 00 00 cc dd
            // server will be stored with keys 0x0000aabb & 0x0000ccdd
            // (or a bit differently based on the little/big indianness of the host)
            string address = node.EndPoint.ToString();
            var fnv = new FNV1a(true);

            for (int i = 0; i < numberOfKeys; i++)
            {
                byte[] data = fnv.ComputeHash(Encoding.UTF8.GetBytes(string.Concat(i, "-", address)));

                for (int h = 0; h < PartCount; h++)
                {
                    k[i * PartCount + h] = BitConverter.ToUInt32(data, h * KeyLength);
                }
            }

            return k;
        }

        #region [ IDisposable                  ]

        void IDisposable.Dispose()
        {
            lock (_deadServerLock)
            {
                // kill all pending operations (with an exception)
                // it's not nice, but disposeing an instance while being used is bad practice
                Interlocked.Exchange(ref _snapshot, null);
            }
        }

        #endregion
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
