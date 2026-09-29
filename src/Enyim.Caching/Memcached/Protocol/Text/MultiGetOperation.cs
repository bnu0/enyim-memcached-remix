using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
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
#if NET8_0_OR_GREATER
            return TextSocketHelper.GetCommandBufferMultiGet(CommandStr, Keys);
#else
            var command = CommandStr + String.Join(" ", Keys.ToArray()) + TextSocketHelper.CommandTerminator;
            return TextSocketHelper.GetCommandBuffer(command);
#endif
        }

        protected internal override IOperationResult ReadResponse(PooledSocket socket)
        {
            _result = new Dictionary<string, CacheItem>(Keys.Count, StringComparer.Ordinal);
            Cas = new Dictionary<string, ulong>(Keys.Count);

            try
            {
                GetHelper.ReadItemsInto(socket, _result, Cas);
            }
            catch (NotSupportedException)
            {
                throw;
            }
            catch (Exception e)
            {
                _log.Error(e);
                return new TextOperationResult().Fail(e.Message ?? "Failed to read multi-get response.", e);
            }

            return new TextOperationResult().Pass();
        }

        Dictionary<string, CacheItem> IMultiGetOperation.Result
        {
            get { return _result; }
        }

        protected internal override ValueTask<IOperationResult> ReadResponseAsync(PooledSocket socket)
        {
            _result = new Dictionary<string, CacheItem>(Keys.Count, StringComparer.Ordinal);
            Cas = new Dictionary<string, ulong>(Keys.Count);

            try
            {
                GetHelper.ReadItemsInto(socket, _result, Cas);
            }
            catch (NotSupportedException)
            {
                throw;
            }
            catch (Exception e)
            {
                _log.Error(e);
                return new ValueTask<IOperationResult>(
                    new TextOperationResult().Fail(e.Message ?? "Failed to read multi-get response.", e));
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
