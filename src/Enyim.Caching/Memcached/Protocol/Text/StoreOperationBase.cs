using System;
using System.Collections.Generic;
using System.Globalization;
using System.Text;
using System.Threading.Tasks;
using Enyim.Caching.Memcached.Results;

namespace Enyim.Caching.Memcached.Protocol.Text
{
    public class StoreOperationBase : SingleItemOperation
    {
        private static readonly ArraySegment<byte> _dataTerminator = new ArraySegment<byte>(new byte[2] { (byte)'\r', (byte)'\n' });
        private readonly StoreCommand _command;
        private CacheItem _value;
        private readonly uint _expires;
        private readonly ulong _cas;

        internal StoreOperationBase(StoreCommand mode, string key, CacheItem value, uint expires, ulong cas)
            : base(key)
        {
            _command = mode;
            _value = value;
            _expires = expires;
            _cas = cas;
        }

        protected internal override System.Collections.Generic.IList<ArraySegment<byte>> GetBuffer()
        {
            var buffers = new List<ArraySegment<byte>>(3);
            string commandPrefix;
            string command;

            switch (_command)
            {
                case StoreCommand.Add: commandPrefix = "add "; break;
                case StoreCommand.Replace: commandPrefix = "replace "; break;
                case StoreCommand.Set: commandPrefix = "set "; break;
                case StoreCommand.Append: commandPrefix = "append "; break;
                case StoreCommand.Prepend: commandPrefix = "prepend "; break;
                case StoreCommand.CheckAndSet: commandPrefix = "cas "; break;
                default: throw new MemcachedClientException(_command + " is not supported.");
            }

            var data = _value.Data;
#if NET8_0_OR_GREATER
            var flagsText = _value.Flags.ToString(CultureInfo.InvariantCulture);
            var expiresText = _expires.ToString(CultureInfo.InvariantCulture);
            var dataLengthText = data.Count.ToString(CultureInfo.InvariantCulture);
            if (_command == StoreCommand.CheckAndSet)
            {
                command = TextSocketHelper.CreateStoreCommand(
                    commandPrefix,
                    Key,
                    flagsText,
                    expiresText,
                    dataLengthText,
                    _cas.ToString(CultureInfo.InvariantCulture));
            }
            else
            {
                command = TextSocketHelper.CreateStoreCommand(
                    commandPrefix,
                    Key,
                    flagsText,
                    expiresText,
                    dataLengthText);
            }
#else
            // todo adjust the size to fit a request using a fnv hashed key
            var sb = new StringBuilder(128);

            sb.Append(commandPrefix);
            sb.Append(Key);
            sb.Append(" ");
            sb.Append(_value.Flags.ToString(CultureInfo.InvariantCulture));
            sb.Append(" ");
            sb.Append(_expires.ToString(CultureInfo.InvariantCulture));
            sb.Append(" ");
            sb.Append(Convert.ToString(data.Count, CultureInfo.InvariantCulture));

            if (_command == StoreCommand.CheckAndSet)
            {
                sb.Append(" ");
                sb.Append(Convert.ToString(_cas, CultureInfo.InvariantCulture));
            }

            sb.Append(TextSocketHelper.CommandTerminator);
            command = sb.ToString();
#endif

            TextSocketHelper.GetCommandBuffer(command, buffers);
            buffers.Add(data);
            buffers.Add(StoreOperationBase._dataTerminator);

            return buffers;
        }

        protected internal override IOperationResult ReadResponse(PooledSocket socket)
        {
            return new TextOperationResult
            {
                Success = String.Compare(TextSocketHelper.ReadResponse(socket), "STORED", StringComparison.Ordinal) == 0
            };
        }

        protected internal override ValueTask<IOperationResult> ReadResponseAsync(PooledSocket socket)
        {
            return new ValueTask<IOperationResult>(new TextOperationResult
            {
                Success = String.Compare(TextSocketHelper.ReadResponse(socket), "STORED", StringComparison.Ordinal) == 0
            });
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
