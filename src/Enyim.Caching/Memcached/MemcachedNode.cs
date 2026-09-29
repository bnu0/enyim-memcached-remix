using Enyim.Caching.Configuration;
using Enyim.Caching.Memcached.Protocol.Binary;
using Enyim.Caching.Memcached.Results;
using Enyim.Caching.Memcached.Results.Extensions;
using Enyim.Collections;

using Microsoft.Extensions.Logging;

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Net;
using System.Net.Security;
using System.Net.Sockets;
using System.Runtime.Serialization;
using System.Security;
using System.Threading;
using System.Threading.Tasks;

namespace Enyim.Caching.Memcached
{
    /// <summary>
    /// Represents a Memcached node in the pool.
    /// </summary>
    [DebuggerDisplay("{{MemcachedNode [ Address: {EndPoint}, IsAlive = {IsAlive} ]}}")]
    public class MemcachedNode : IMemcachedNode
    {
        private readonly ILogger _logger;
        private static readonly object SyncRoot = new();
        private bool _isDisposed;
        private readonly EndPoint _endPoint;
        private readonly ISocketPoolConfiguration _config;
        private InternalPoolImpl _internalPoolImpl;
        private bool _isInitialized = false;
        private SemaphoreSlim poolInitSemaphore = new SemaphoreSlim(1, 1);
        private readonly TimeSpan _initPoolTimeout;
        private bool _useSslStream;
        private bool _useIPv6;
        
        public MemcachedNode(
            EndPoint endpoint,
            ISocketPoolConfiguration socketPoolConfig,
            ILogger logger,
            bool useSslStream,
            bool useIPv6)
        {
            _endPoint = endpoint;
            _useSslStream = useSslStream;
            EndPointString = endpoint?.ToString().Replace("Unspecified/", string.Empty);
            _config = socketPoolConfig;

            if (socketPoolConfig.ConnectionTimeout.TotalMilliseconds >= int.MaxValue)
                throw new InvalidOperationException("ConnectionTimeout must be < int.MaxValue");

            if (socketPoolConfig.InitPoolTimeout.TotalSeconds < 1)
            {
                _initPoolTimeout = new TimeSpan(0, 1, 0);
            }
            else
            {
                _initPoolTimeout = socketPoolConfig.InitPoolTimeout;
            }

            _logger = logger;
            _internalPoolImpl = new InternalPoolImpl(this, socketPoolConfig, _logger);
            _useIPv6 = useIPv6;
        }

        public event Action<IMemcachedNode> Failed;
        private INodeFailurePolicy _failurePolicy;

        protected INodeFailurePolicy FailurePolicy
        {
            get { return _failurePolicy ?? (_failurePolicy = _config.FailurePolicyFactory.Create(this)); }
        }

        /// <summary>
        /// Gets the <see cref="T:IPEndPoint"/> of this instance
        /// </summary>
        public EndPoint EndPoint
        {
            get { return _endPoint; }
        }

        public string EndPointString { get; private set; }

        /// <summary>
        /// <para>Gets a value indicating whether the server is working or not. Returns a <b>cached</b> state.</para>
        /// <para>To get real-time information and update the cached state, use the <see cref="M:Ping"/> method.</para>
        /// </summary>
        /// <remarks>Used by the <see cref="T:ServerPool"/> to quickly check if the server's state is valid.</remarks>
        public bool IsAlive
        {
            get { return _internalPoolImpl.IsAlive; }
        }

        /// <summary>
        /// Gets a value indicating whether the server is working or not.
        ///
        /// If the server is back online, we'll ercreate the internal socket pool and mark the server as alive so operations can target it.
        /// </summary>
        /// <returns>true if the server is alive; false otherwise.</returns>
        public bool Ping()
        {
            // is the server working?
            if (_internalPoolImpl.IsAlive)
                return true;

            // this codepath is (should be) called very rarely
            // if you get here hundreds of times then you have bigger issues
            // and try to make the memcached instaces more stable and/or increase the deadTimeout
            try
            {
                // we could connect to the server, let's recreate the socket pool
                lock (SyncRoot)
                {
                    if (_isDisposed) return false;

                    // try to connect to the server
                    using (var socket = CreateSocket())
                    {
                    }

                    if (_internalPoolImpl.IsAlive)
                        return true;

                    // it's easier to create a new pool than reinitializing a dead one
                    // rewrite-then-dispose to avoid a race condition with Acquire (which does no locking)
                    var oldPool = _internalPoolImpl;
                    var newPool = new InternalPoolImpl(this, _config, _logger);

                    Interlocked.Exchange(ref _internalPoolImpl, newPool);

                    try { oldPool.Dispose(); }
                    catch { }
                }

                return true;
            }
            //could not reconnect
            catch { return false; }
        }

        /// <summary>
        /// Acquires a new item from the pool
        /// </summary>
        /// <returns>An <see cref="T:PooledSocket"/> instance which is connected to the memcached server, or <value>null</value> if the pool is dead.</returns>
        public IPooledSocketResult Acquire()
        {
            var result = new PooledSocketResult();
            if (!_isInitialized)
            {
                if (!poolInitSemaphore.Wait(_initPoolTimeout))
                {
                    return result.Fail("Timeout to poolInitSemaphore.Wait", _logger) as PooledSocketResult;
                }

                try
                {
                    if (!_isInitialized)
                    {
                        var startTime = DateTime.Now;
                        _internalPoolImpl.InitPool();
                        _isInitialized = true;

                        if (_logger.IsEnabled(LogLevel.Information))
                        {
                            _logger.LogInformation("MemcachedInitPool-cost: {0}ms", (DateTime.Now - startTime).TotalMilliseconds);
                        }
                    }
                }
                finally
                {
                    poolInitSemaphore.Release();
                }
            }

            try
            {
                return _internalPoolImpl.Acquire();
            }
            catch (Exception e)
            {
                var message = "Acquire failed. Maybe we're already disposed?";
                _logger.LogError(message, e);

                result.Fail(message, e);
                return result;
            }
        }

        /// <summary>
        /// Acquires a new item from the pool
        /// </summary>
        /// <returns>An <see cref="T:PooledSocket"/> instance which is connected to the memcached server, or <value>null</value> if the pool is dead.</returns>
        public async Task<IPooledSocketResult> AcquireAsync()
        {
            var result = new PooledSocketResult();
            if (!_isInitialized)
            {
                if (!await poolInitSemaphore.WaitAsync(_initPoolTimeout))
                {
                    return result.Fail("Timeout to poolInitSemaphore.Wait", _logger) as PooledSocketResult;
                }

                try
                {
                    if (!_isInitialized)
                    {
                        var startTime = DateTime.Now;
                        await _internalPoolImpl.InitPoolAsync();
                        _isInitialized = true;

                        if (_logger.IsEnabled(LogLevel.Information))
                        {
                            _logger.LogInformation("MemcachedInitPool-cost: {0}ms", (DateTime.Now - startTime).TotalMilliseconds);
                        }
                    }
                }
                finally
                {
                    poolInitSemaphore.Release();
                }
            }

            try
            {
                return await _internalPoolImpl.AcquireAsync();
            }
            catch (Exception e)
            {
                var message = "Acquire failed. Maybe we're already disposed?";
                _logger.LogError(message, e);
                result.Fail(message, e);
                return result;
            }
        }

        ~MemcachedNode()
        {
            try { ((IDisposable)this).Dispose(); }
            catch { }
        }

        /// <summary>
        /// Releases all resources allocated by this instance
        /// </summary>
        public void Dispose()
        {
            if (_isDisposed) return;

            GC.SuppressFinalize(this);

            // this is not a graceful shutdown
            // if someone uses a pooled item then it's 99% that an exception will be thrown
            // somewhere. But since the dispose is mostly used when everyone else is finished
            // this should not kill any kittens
            lock (SyncRoot)
            {
                if (_isDisposed) return;

                _isDisposed = true;
                _internalPoolImpl.Dispose();
                poolInitSemaphore.Dispose();
            }
        }

        void IDisposable.Dispose()
        {
            Dispose();
        }

        #region [ InternalPoolImpl             ]

        private class InternalPoolImpl : IDisposable
        {
            private readonly ILogger _logger;
            private readonly bool _isDebugEnabled;

            private bool _isDisposed;
            private bool _isAlive;
            private DateTime _markedAsDeadUtc;

            private readonly int _minItems;
            private readonly int _maxItems;

            private MemcachedNode _ownerNode;
            private readonly EndPoint _endPoint;
            private readonly TimeSpan _queueTimeout;
            private readonly TimeSpan _receiveTimeout;
            private StripedSocketPool _sockets;

            private readonly object initLock = new Object();

            internal InternalPoolImpl(
                MemcachedNode ownerNode,
                ISocketPoolConfiguration config,
                ILogger logger)
            {
                if (config.MinPoolSize < 0)
                    throw new InvalidOperationException("minItems must be larger >= 0", null);
                if (config.MaxPoolSize < config.MinPoolSize)
                    throw new InvalidOperationException("maxItems must be larger than minItems", null);
                if (config.QueueTimeout < TimeSpan.Zero)
                    throw new InvalidOperationException("queueTimeout must be >= TimeSpan.Zero", null);
                if (config.ReceiveTimeout < TimeSpan.Zero)
                    throw new InvalidOperationException("ReceiveTimeout must be >= TimeSpan.Zero", null);
                if (config.ConnectionIdleTimeout < TimeSpan.Zero)
                    throw new InvalidOperationException("ConnectionIdleTimeout must be >= TimeSpan.Zero", null);

                _ownerNode = ownerNode;
                _isAlive = true;
                _endPoint = ownerNode.EndPoint;
                _queueTimeout = config.QueueTimeout;
                _receiveTimeout = config.ReceiveTimeout;

                _minItems = config.MinPoolSize;
                _maxItems = config.MaxPoolSize;

                _sockets = new StripedSocketPool(_maxItems, config.ConnectionIdleTimeout, logger);

                _logger = logger;
                _isDebugEnabled = _logger.IsEnabled(LogLevel.Debug);
            }

            internal void InitPool()
            {
                try
                {
                    if (_minItems > 0)
                    {
                        for (int i = 0; i < _minItems; i++)
                        {
                            try
                            {
                                if (_sockets.TryReserveSlot())
                                {
                                    try
                                    {
                                        _sockets.Enqueue(CreateSocket());
                                    }
                                    catch
                                    {
                                        _sockets.ReleaseReservedSlot();
                                        throw;
                                    }
                                }
                            }
                            catch (Exception ex)
                            {
                                _logger.LogError(ex, $"Failed to put {nameof(PooledSocket)} {i} in Pool");
                            }

                            // cannot connect to the server
                            if (!_isAlive)
                                break;
                        }
                    }

                    if (_logger.IsEnabled(LogLevel.Debug))
                        _logger.LogDebug("Pool has been inited for {0} with {1} sockets", _endPoint, _minItems);

                }
                catch (Exception e)
                {
                    _logger.LogError(e, "Could not init pool.");

                    MarkAsDead();
                }
            }

            internal async Task InitPoolAsync()
            {
                try
                {
                    if (_minItems > 0)
                    {
                        for (int i = 0; i < _minItems; i++)
                        {
                            try
                            {
                                if (_sockets.TryReserveSlot())
                                {
                                    try
                                    {
                                        _sockets.Enqueue(await CreateSocketAsync());
                                    }
                                    catch
                                    {
                                        _sockets.ReleaseReservedSlot();
                                        throw;
                                    }
                                }
                            }
                            catch (Exception ex)
                            {
                                _logger.LogError(ex, $"Failed to put {nameof(PooledSocket)} {i} in Pool");
                            }

                            // cannot connect to the server
                            if (!_isAlive)
                                break;
                        }
                    }

                    if (_logger.IsEnabled(LogLevel.Debug))
                        _logger.LogDebug("Pool has been inited for {0} with {1} sockets", _endPoint, _minItems);

                }
                catch (Exception e)
                {
                    _logger.LogError(e, "Could not init pool.");

                    MarkAsDead();
                }
            }

            private async Task<PooledSocket> CreateSocketAsync()
            {
                var ps = await _ownerNode.CreateSocketAsync();
                ps.CleanupCallback = ReleaseSocket;

                return ps;
            }

            private PooledSocket CreateSocket()
            {
                var ps = _ownerNode.CreateSocket();
                ps.CleanupCallback = ReleaseSocket;

                return ps;
            }

            public bool IsAlive
            {
                get { return _isAlive; }
            }

            public DateTime MarkedAsDeadUtc
            {
                get { return _markedAsDeadUtc; }
            }

            /// <summary>
            /// Acquires a new item from the pool
            /// </summary>
            /// <returns>An <see cref="T:PooledSocket"/> instance which is connected to the memcached server, or <value>null</value> if the pool is dead.</returns>
            public IPooledSocketResult Acquire()
            {
                var result = new PooledSocketResult();
                if (_isDebugEnabled) _logger.LogDebug($"Acquiring stream from pool on node '{_endPoint}'");

                if (!_isAlive || _isDisposed)
                {
                    return FailDeadOrDisposed(result);
                }

                if (_sockets.TryTake(out var socket))
                {
                    return CompleteAcquireFromPool(result, socket);
                }

                if (TryCreateSocket(result, out socket))
                {
                    return result;
                }

                return WaitForSocket(result);
            }

            /// <summary>
            /// Acquires a new item from the pool
            /// </summary>
            /// <returns>An <see cref="T:PooledSocket"/> instance which is connected to the memcached server, or <value>null</value> if the pool is dead.</returns>
            public async Task<IPooledSocketResult> AcquireAsync()
            {
                var result = new PooledSocketResult();

                if (_isDebugEnabled) _logger.LogDebug("Acquiring stream from pool. " + _endPoint);

                if (!_isAlive || _isDisposed)
                {
                    return FailDeadOrDisposed(result);
                }

                if (_sockets.TryTake(out var socket))
                {
                    return await CompleteAcquireFromPoolAsync(result, socket).ConfigureAwait(false);
                }

                if (await TryCreateSocketAsync(result).ConfigureAwait(false))
                {
                    return result;
                }

                return await WaitForSocketAsync(result).ConfigureAwait(false);
            }

            private IPooledSocketResult FailDeadOrDisposed(PooledSocketResult result)
            {
                var message = "Pool is dead or disposed, returning null. " + _endPoint;
                result.Fail(message);

                if (_isDebugEnabled) _logger.LogDebug(message);

                return result;
            }

            private IPooledSocketResult CompleteAcquireFromPool(PooledSocketResult result, PooledSocket socket)
            {
                try
                {
                    socket.Reset();

                    var message = "Socket was reset. " + socket.InstanceId;
                    if (_isDebugEnabled) _logger.LogDebug(message);

                    result.Pass(message);
                    socket.UpdateLastUsed();
                    result.Value = socket;
                    return result;
                }
                catch (Exception e)
                {
                    var message = "Failed to reset an acquired socket.";
                    _logger.LogError(message, e);

                    DiscardCheckedOutSocket(socket);
                    MarkAsDead();

                    result.Fail(message, e);
                    return result;
                }
            }

            private async Task<IPooledSocketResult> CompleteAcquireFromPoolAsync(PooledSocketResult result, PooledSocket socket)
            {
                try
                {
                    var resetTask = socket.ResetAsync();

                    if (await Task.WhenAny(resetTask, Task.Delay(_receiveTimeout)).ConfigureAwait(false) == resetTask)
                    {
                        await resetTask.ConfigureAwait(false);
                    }
                    else
                    {
                        socket.IsAlive = false;
                        DiscardCheckedOutSocket(socket);

                        var timeoutMessage = "Timeout to reset an acquired socket. InstanceId " + socket.InstanceId;
                        _logger.LogError(timeoutMessage);
                        result.Fail(timeoutMessage);
                        return result;
                    }

                    var message = "Socket was reset. InstanceId " + socket.InstanceId;
                    if (_isDebugEnabled) _logger.LogDebug(message);

                    result.Pass(message);
                    socket.UpdateLastUsed();
                    result.Value = socket;
                    return result;
                }
                catch (Exception e)
                {
                    DiscardCheckedOutSocket(socket);
                    MarkAsDead();

                    var message = "Failed to reset an acquired socket.";
                    _logger.LogError(message, e);
                    result.Fail(message, e);
                    return result;
                }
            }

            private bool TryCreateSocket(PooledSocketResult result, out PooledSocket socket)
            {
                socket = null;
                if (!_sockets.TryReserveSlot())
                {
                    return false;
                }

                var message = "Could not get a socket from the pool, Creating a new item. " + _endPoint;
                if (_isDebugEnabled) _logger.LogDebug(message);

                try
                {
                    var startTime = DateTime.Now;
                    socket = CreateSocket();

                    if (_logger.IsEnabled(LogLevel.Information))
                    {
                        _logger.LogInformation("MemcachedAcquire-CreateSocket: {0}ms", (DateTime.Now - startTime).TotalMilliseconds);
                    }

                    result.Value = socket;
                    result.Pass();
                    return true;
                }
                catch (Exception e)
                {
                    _sockets.ReleaseReservedSlot();

                    message = "Failed to create socket. " + _endPoint;
                    _logger.LogError(message, e);

                    MarkAsDead();
                    result.Fail(message, e);
                    socket = null;
                    return true;
                }
            }

            private async Task<bool> TryCreateSocketAsync(PooledSocketResult result)
            {
                if (!_sockets.TryReserveSlot())
                {
                    return false;
                }

                var message = "Could not get a socket from the pool, Creating a new item. " + _endPoint;
                if (_isDebugEnabled) _logger.LogDebug(message);

                try
                {
                    var startTime = DateTime.Now;
                    var created = await CreateSocketAsync().ConfigureAwait(false);

                    if (_logger.IsEnabled(LogLevel.Information))
                    {
                        _logger.LogInformation("MemcachedAcquire-CreateSocket: {0}ms", (DateTime.Now - startTime).TotalMilliseconds);
                    }

                    result.Value = created;
                    result.Pass();
                    return true;
                }
                catch (Exception e)
                {
                    _sockets.ReleaseReservedSlot();

                    message = "Failed to create socket. " + _endPoint;
                    _logger.LogError(message, e);

                    MarkAsDead();
                    result.Fail(message);
                    return true;
                }
            }

            private IPooledSocketResult WaitForSocket(PooledSocketResult result)
            {
                _sockets.EnterWait();
                try
                {
                    var started = DateTime.UtcNow;
                    while (true)
                    {
                        if (!_isAlive || _isDisposed)
                        {
                            return FailDeadOrDisposed(result);
                        }

                        if (_sockets.TryTake(out var socket))
                        {
                            return CompleteAcquireFromPool(result, socket);
                        }

                        if (TryCreateSocket(result, out _))
                        {
                            return result;
                        }

                        var remaining = _queueTimeout - (DateTime.UtcNow - started);
                        if (remaining <= TimeSpan.Zero || !_sockets.Wait(remaining))
                        {
                            var message = "Pool is full, timeouting. " + _endPoint;
                            if (_isDebugEnabled) _logger.LogDebug(message);
                            result.Fail(message, new TimeoutException());
                            return result;
                        }
                    }
                }
                finally
                {
                    _sockets.ExitWait();
                }
            }

            private async Task<IPooledSocketResult> WaitForSocketAsync(PooledSocketResult result)
            {
                _sockets.EnterWait();
                try
                {
                    var started = DateTime.UtcNow;
                    while (true)
                    {
                        if (!_isAlive || _isDisposed)
                        {
                            return FailDeadOrDisposed(result);
                        }

                        if (_sockets.TryTake(out var socket))
                        {
                            return await CompleteAcquireFromPoolAsync(result, socket).ConfigureAwait(false);
                        }

                        if (await TryCreateSocketAsync(result).ConfigureAwait(false))
                        {
                            return result;
                        }

                        var remaining = _queueTimeout - (DateTime.UtcNow - started);
                        if (remaining <= TimeSpan.Zero || !await _sockets.WaitAsync(remaining).ConfigureAwait(false))
                        {
                            var message = "Pool is full, timeouting. " + _endPoint;
                            if (_isDebugEnabled) _logger.LogDebug(message);
                            result.Fail(message, new TimeoutException());
                            return result;
                        }
                    }
                }
                finally
                {
                    _sockets.ExitWait();
                }
            }

            private void DiscardCheckedOutSocket(PooledSocket socket)
            {
                try
                {
                    socket.Destroy();
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, $"Failed to destroy {nameof(PooledSocket)}");
                }

                _sockets.ReleaseReservedSlot();
            }

            private void MarkAsDead()
            {
                if (_isDebugEnabled) _logger.LogDebug("Mark as dead was requested for {0}", _endPoint);

                var shouldFail = _ownerNode.FailurePolicy.ShouldFail();

                if (_isDebugEnabled) _logger.LogDebug("FailurePolicy.ShouldFail(): " + shouldFail);

                if (shouldFail)
                {
                    if (_logger.IsEnabled(LogLevel.Warning)) _logger.LogWarning("Marking node {0} as dead", _endPoint);

                    _isAlive = false;
                    _markedAsDeadUtc = DateTime.UtcNow;

                    var f = _ownerNode.Failed;

                    if (f != null)
                        f(_ownerNode);
                }
            }

            /// <summary>
            /// Releases an item back into the pool
            /// </summary>
            /// <param name="socket"></param>
            private void ReleaseSocket(PooledSocket socket)
            {
                if (_isDebugEnabled)
                {
                    _logger.LogDebug("Releasing socket " + socket.InstanceId);
                    _logger.LogDebug("Are we alive? " + _isAlive);
                }

                if (_isDisposed)
                {
                    DiscardCheckedOutSocket(socket);
                    return;
                }

                if (_isAlive)
                {
                    if (socket.IsAlive)
                    {
                        _sockets.Enqueue(socket);
                    }
                    else
                    {
                        DiscardCheckedOutSocket(socket);
                        MarkAsDead();
                    }
                }
                else
                {
                    // one of our previous sockets has died, so probably all of them
                    // are dead. so, kill the socket (this will eventually clear the pool as well)
                    DiscardCheckedOutSocket(socket);
                }
            }

            ~InternalPoolImpl()
            {
                try { ((IDisposable)this).Dispose(); }
                catch { }
            }

            /// <summary>
            /// Releases all resources allocated by this instance
            /// </summary>
            public void Dispose()
            {
                // this is not a graceful shutdown
                // if someone uses a pooled item then 99% that an exception will be thrown
                // somewhere. But since the dispose is mostly used when everyone else is finished
                // this should not kill any kittens
                if (!_isDisposed)
                {
                    _isAlive = false;
                    _isDisposed = true;

                    _sockets.Dispose();
                    _ownerNode = null;
                }
            }

            void IDisposable.Dispose()
            {
                Dispose();
            }
        }

        #endregion
        #region [ Comparer                     ]
        internal sealed class Comparer : IEqualityComparer<IMemcachedNode>
        {
            public static readonly Comparer Instance = new Comparer();

            bool IEqualityComparer<IMemcachedNode>.Equals(IMemcachedNode x, IMemcachedNode y)
            {
                return x.EndPoint.Equals(y.EndPoint);
            }

            int IEqualityComparer<IMemcachedNode>.GetHashCode(IMemcachedNode obj)
            {
                return obj.EndPoint.GetHashCode();
            }
        }
        #endregion

        protected internal virtual PooledSocket CreateSocket()
        {
            try
            {
                var ps = new PooledSocket(_endPoint, _config.ConnectionTimeout, _config.ReceiveTimeout, _logger, _useSslStream, _useIPv6);
                ps.Connect();
                return ps;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"Create {nameof(PooledSocket)}");
                throw;
            }

        }

        protected internal virtual async Task<PooledSocket> CreateSocketAsync()
        {
            try
            {
                var ps = new PooledSocket(_endPoint, _config.ConnectionTimeout, _config.ReceiveTimeout, _logger, _useSslStream, _useIPv6);
                await ps.ConnectAsync();
                return ps;
            }
            catch (Exception ex)
            {
                var endPointStr = _endPoint.ToString().Replace("Unspecified/", string.Empty);
                _logger.LogError(ex, $"Failed to {nameof(CreateSocketAsync)} to {endPointStr}");
                throw;
            }
        }

        //protected internal virtual PooledSocket CreateSocket(IPEndPoint endpoint, TimeSpan connectionTimeout, TimeSpan receiveTimeout)
        //{
        //    PooledSocket retval = new PooledSocket(endPoint, connectionTimeout, receiveTimeout);

        //    return retval;
        //}

        protected virtual IPooledSocketResult ExecuteOperation(IOperation op)
        {
            var result = Acquire();
            if (result.Success && result.HasValue)
            {
                try
                {
                    var socket = result.Value;
                    //if Get, call BinaryRequest.CreateBuffer()
                    var b = op.GetBuffer();

                    var startTime = DateTime.Now;
                    socket.Write(b);
                    LogExecutionTime("ExecuteOperation_socket_write", startTime, 50);

                    //if Get, call BinaryResponse
                    var readResult = op.ReadResponse(socket);
                    if (readResult.Success)
                    {
                        result.Pass();
                    }
                    else
                    {
                        readResult.Combine(result);
                    }
                    return result;
                }
                catch (IOException e)
                {
                    _logger.LogError(e, $"Failed to ExecuteOperation on {EndPointString}");

                    result.Fail("Exception reading response", e);
                    return result;
                }
                finally
                {
                    ((IDisposable)result.Value).Dispose();
                }
            }
            else
            {
                var errorMsg = string.IsNullOrEmpty(result.Message) ? "Failed to acquire a socket from pool" : result.Message;
                _logger.LogError(errorMsg);
                return result;
            }

        }

        protected virtual async Task<IPooledSocketResult> ExecuteOperationAsync(IOperation op)
        {
            if (_logger.IsEnabled(LogLevel.Debug))
            {
                _logger.LogDebug($"ExecuteOperationAsync({op})");
            }

            var result = await AcquireAsync();
            if (result.Success && result.HasValue)
            {
                try
                {
                    var pooledSocket = result.Value;

                    //if Get, call BinaryRequest.CreateBuffer()
                    var b = op.GetBuffer();

                    if (_logger.IsEnabled(LogLevel.Debug))
                    {
                        _logger.LogDebug("pooledSocket.WriteAsync...");
                    }

                    var writeSocketTask = pooledSocket.WriteAsync(b);
                    if (await Task.WhenAny(writeSocketTask, Task.Delay(_config.ConnectionTimeout)) != writeSocketTask)
                    {
                        result.Fail("Timeout to pooledSocket.WriteAsync");
                        return result;
                    }
                    await writeSocketTask;

                    //if Get, call BinaryResponse
                    if (_logger.IsEnabled(LogLevel.Debug))
                    {
                        _logger.LogDebug($"{op}.ReadResponseAsync...");
                    }

                    var readResponseTask = op.ReadResponseAsync(pooledSocket);
                    if (await Task.WhenAny(readResponseTask, Task.Delay(_config.ConnectionTimeout)) != readResponseTask)
                    {
                        result.Fail($"Timeout to ReadResponseAsync(pooledSocket) for {op}");
                        return result;
                    }

                    var readResult = await readResponseTask;
                    if (readResult.Success)
                    {
                        result.Pass();
                    }
                    else
                    {
                        if (_logger.IsEnabled(LogLevel.Debug))
                        {
                            _logger.LogDebug($"{op}.{nameof(op.ReadResponseAsync)} result: {readResult.Message}");
                        }

                        readResult.Combine(result);
                    }
                    return result;
                }
                catch (IOException e)
                {
                    _logger.LogError(e, $"IOException occurs when ExecuteOperationAsync({op}) on {EndPointString}");

                    result.Fail("IOException reading response", e);
                    return result;
                }
                catch (SocketException e)
                {
                    _logger.LogError(e, $"SocketException occurs when ExecuteOperationAsync({op}) on {EndPointString}");

                    result.Fail("SocketException reading response", e);
                    return result;
                }
                finally
                {
                    ((IDisposable)result.Value).Dispose();
                }
            }
            else
            {
                var errorMsg = string.IsNullOrEmpty(result.Message) ? "Failed to acquire a socket from pool" : result.Message;
                _logger.LogError(errorMsg);
                return result;
            }
        }

        protected virtual async Task<bool> ExecuteOperationAsync(IOperation op, Action<bool> next)
        {
            var socket = (await AcquireAsync()).Value;
            if (socket == null) return false;

            //key(string) to buffer(btye[])
            var b = op.GetBuffer();

            try
            {
                await socket.WriteAsync(b);

                var rrs = await op.ReadResponseAsync(socket, readSuccess =>
                {
                    ((IDisposable)socket).Dispose();

                    next(readSuccess);
                });

                return rrs;
            }
            catch (IOException e)
            {
                _logger.LogError(e, $"Failed to ExecuteOperationAsync({op}) with next action on {EndPointString}");
                ((IDisposable)socket).Dispose();

                return false;
            }
        }

        private void LogExecutionTime(string title, DateTime startTime, int thresholdMs)
        {
            var duration = (DateTime.Now - startTime).TotalMilliseconds;
            if (duration > thresholdMs)
            {
                _logger.LogWarning("MemcachedNode-{0}: {1}ms", title, duration);
            }
        }

        #region [ IMemcachedNode               ]

        EndPoint IMemcachedNode.EndPoint
        {
            get { return _endPoint; }
        }

        bool IMemcachedNode.IsAlive
        {
            get { return IsAlive; }
        }

        bool IMemcachedNode.Ping()
        {
            return Ping();
        }

        IOperationResult IMemcachedNode.Execute(IOperation op)
        {
            return ExecuteOperation(op);
        }

        async Task<IOperationResult> IMemcachedNode.ExecuteAsync(IOperation op)
        {
            return await ExecuteOperationAsync(op);
        }

        async Task<bool> IMemcachedNode.ExecuteAsync(IOperation op, Action<bool> next)
        {
            return await ExecuteOperationAsync(op, next);
        }

        event Action<IMemcachedNode> IMemcachedNode.Failed
        {
            add { Failed += value; }
            remove { Failed -= value; }
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
