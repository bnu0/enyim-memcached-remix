using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Collections.Pooled;
using Enyim.Caching.Memcached.Results;
using Enyim.Caching.Memcached.Results.Extensions;

namespace Enyim.Caching.Memcached.Protocol.Text
{
    public class MultiGetOperation : MultiItemOperation, IMultiGetOperation
    {
        private const string CommandStr = "gets ";
        private static readonly ILog _log = LogManager.GetLogger(typeof(MultiGetOperation));

        private Dictionary<string, CacheItem> _result;

        public MultiGetOperation(IList<string> keys) : base(keys) { }

        protected internal override IList<ArraySegment<byte>> GetBuffer()
        {
            // gets key1 key2 key3 ... keyN\r\n
            string command;
#if NET8_0_OR_GREATER
            int totalLength = Keys.Sum(s => s.Length) + (Keys.Count - 1) + CommandStr.Length;

            command = string.Create(totalLength, Keys, (span, state) =>
            {
                int position = 0;
                CommandStr.AsSpan().CopyTo(span.Slice(position));
                position += CommandStr.Length;
                for (int i = 0; i < state.Count; i++)
                {
                    if (i > 0) span[position++] = ',';
                    state[i].CopyTo(span.Slice(position));
                    position += state[i].Length;
                }
            });
#else
            command = CommandStr + String.Join(" ", Keys.ToArray()) + TextSocketHelper.CommandTerminator;
#endif
            return TextSocketHelper.GetCommandBuffer(command);
        }

        protected internal override IOperationResult ReadResponse(PooledSocket socket)
        {
            using (var retval = new PooledDictionary<string, CacheItem>())
            using (var cas = new PooledDictionary<string, ulong>())
            {
                try
                {
                    GetResponse r;

                    while ((r = GetHelper.ReadItem(socket)) != null)
                    {
                        var key = r.Key;

                        retval[key] = r.Item;
                        cas[key] = r.CasValue;
                    }
                }
                catch (NotSupportedException)
                {
                    throw;
                }
                catch (Exception e)
                {
                    _log.Error(e);
                }

                _result = new Dictionary<string, CacheItem>(retval);
                Cas = new Dictionary<string, ulong>(cas);
            }

            return new TextOperationResult().Pass();
        }

        Dictionary<string, CacheItem> IMultiGetOperation.Result
        {
            get { return _result; }
        }

        protected internal override ValueTask<IOperationResult> ReadResponseAsync(PooledSocket socket)
        {
            using (var retval = new PooledDictionary<string, CacheItem>())
            using (var cas = new PooledDictionary<string, ulong>())
            {
                try
                {
                    GetResponse r;

                    while ((r = GetHelper.ReadItem(socket)) != null)
                    {
                        var key = r.Key;

                        retval[key] = r.Item;
                        cas[key] = r.CasValue;
                    }
                }
                catch (NotSupportedException)
                {
                    throw;
                }
                catch (Exception e)
                {
                    _log.Error(e);
                }

                _result = new Dictionary<string, CacheItem>(retval);
                Cas = new Dictionary<string, ulong>(cas);
            }

            return new ValueTask<IOperationResult>(new TextOperationResult().Pass());
        }

        protected internal override Task<bool> ReadResponseAsync(PooledSocket socket, System.Action<bool> next)
        {
            throw new System.NotSupportedException();
        }
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
