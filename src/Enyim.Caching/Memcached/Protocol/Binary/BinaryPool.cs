using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Net;
using System.Threading;
using Enyim.Caching.Configuration;
using Enyim.Collections;
using System.Security;
using Microsoft.Extensions.Logging;

namespace Enyim.Caching.Memcached.Protocol.Binary
{
    /// <summary>
    /// Server pool implementing the binary protocol.
    /// </summary>
    public class BinaryPool : DefaultServerPool
    {
        readonly ISaslAuthenticationProvider _authenticationProvider;
        readonly IMemcachedClientConfiguration _configuration;
        private readonly ILogger _logger;

        public BinaryPool(IMemcachedClientConfiguration configuration, ILogger logger)
            : base(configuration, new BinaryOperationFactory(logger), logger)
        {
            _authenticationProvider = GetProvider(configuration);
            _configuration = configuration;
            _logger = logger;
        }

        protected override IMemcachedNode CreateNode(EndPoint endpoint)
        {
            return new BinaryNode(endpoint, _configuration.SocketPool, _authenticationProvider, _logger, _configuration.UseSslStream, _configuration.UseIPv6);
        }

        private static ISaslAuthenticationProvider GetProvider(IMemcachedClientConfiguration configuration)
        {
            // create&initialize the authenticator, if any
            // we'll use this single instance everywhere, so it must be thread safe
            IAuthenticationConfiguration auth = configuration.Authentication;
            if (auth != null)
            {
                Type t = auth.Type;
                var provider = (t == null) ? null : Enyim.Reflection.FastActivator.Create(t) as ISaslAuthenticationProvider;

                if (provider != null)
                {
                    provider.Initialize(auth.Parameters);
                    return provider;
                }
            }

            return null;
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
