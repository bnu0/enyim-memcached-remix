using Microsoft.Extensions.Logging;
using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;

namespace Enyim.Caching.Memcached
{
    /// <summary>
    /// Free-list and occupancy cap for one node's sockets. Reuse is lock-free per stripe;
    /// <see cref="Wait"/> is used only when every stripe is empty and live sockets are at max.
    /// </summary>
    internal sealed class StripedSocketPool : IDisposable
    {
        private const int MaxStripeCount = 16;

        private readonly ConcurrentQueue<PooledSocket>[] _stripes;
        private readonly int _stripeCount;
        private readonly int _maxItems;
        private readonly TimeSpan _connectionIdleTimeout;
        private readonly ILogger _logger;
        private readonly SemaphoreSlim _available;

        private int _live;
        private int _waiters;
        private int _disposed;

        public StripedSocketPool(
            int maxItems,
            TimeSpan connectionIdleTimeout,
            ILogger logger)
        {
            _maxItems = maxItems;
            _connectionIdleTimeout = connectionIdleTimeout;
            _logger = logger;
            _stripeCount = ResolveStripeCount(maxItems);
            _stripes = new ConcurrentQueue<PooledSocket>[_stripeCount];
            for (int i = 0; i < _stripeCount; i++)
            {
                _stripes[i] = new ConcurrentQueue<PooledSocket>();
            }

            _available = new SemaphoreSlim(0, Math.Max(1, maxItems));
        }

        public int StripeCount => _stripeCount;

        public int Live => Volatile.Read(ref _live);

        public static int StripeIndex(int stripeCount)
        {
            return Environment.CurrentManagedThreadId % stripeCount;
        }

        public static int ResolveStripeCount(int maxItems)
        {
            if (maxItems <= 1)
            {
                return 1;
            }

            var n = Environment.ProcessorCount;
            if (n < 1)
            {
                n = 1;
            }

            if (n > MaxStripeCount)
            {
                n = MaxStripeCount;
            }

            return n > maxItems ? maxItems : n;
        }

        /// <summary>
        /// Reserves a live slot for a socket that is about to be created. Caller must
        /// <see cref="Enqueue"/> it or <see cref="ReleaseReservedSlot"/> if create fails.
        /// </summary>
        public bool TryReserveSlot()
        {
            var n = Interlocked.Increment(ref _live);
            if (n <= _maxItems)
            {
                return true;
            }

            Interlocked.Decrement(ref _live);
            return false;
        }

        public void ReleaseReservedSlot()
        {
            Interlocked.Decrement(ref _live);
            SignalWaiters();
        }

        public void Enqueue(PooledSocket socket)
        {
            if (Volatile.Read(ref _disposed) != 0)
            {
                try
                {
                    socket.Destroy();
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, $"Failed to destroy {nameof(PooledSocket)}");
                }

                ReleaseReservedSlot();
                return;
            }

            _stripes[StripeIndex(_stripeCount)].Enqueue(socket);
            SignalWaiters();
        }

        public bool TryTake(out PooledSocket socket)
        {
            var start = StripeIndex(_stripeCount);
            for (int i = 0; i < _stripeCount; i++)
            {
                var q = _stripes[(start + i) % _stripeCount];
                while (q.TryDequeue(out socket))
                {
                    if (ShouldDiscard(socket))
                    {
                        Discard(socket);
                        continue;
                    }

                    return true;
                }
            }

            socket = null;
            return false;
        }

        public void EnterWait()
        {
            Interlocked.Increment(ref _waiters);
        }

        public void ExitWait()
        {
            Interlocked.Decrement(ref _waiters);
        }

        public bool Wait(TimeSpan timeout)
        {
            try
            {
                return _available.Wait(timeout);
            }
            catch (ObjectDisposedException)
            {
                return false;
            }
        }

        public async Task<bool> WaitAsync(TimeSpan timeout)
        {
            try
            {
                return await _available.WaitAsync(timeout).ConfigureAwait(false);
            }
            catch (ObjectDisposedException)
            {
                return false;
            }
        }

        public void SignalWaiters()
        {
            if (Volatile.Read(ref _disposed) != 0)
            {
                return;
            }

            if (Volatile.Read(ref _waiters) > 0)
            {
                try
                {
                    _available.Release();
                }
                catch (SemaphoreFullException)
                {
                }
                catch (ObjectDisposedException)
                {
                }
            }
        }

        public void Dispose()
        {
            if (Interlocked.Exchange(ref _disposed, 1) != 0)
            {
                return;
            }

            try
            {
                var waiters = Volatile.Read(ref _waiters);
                if (waiters > 0)
                {
                    try
                    {
                        _available.Release(waiters);
                    }
                    catch (SemaphoreFullException)
                    {
                    }
                    catch (ObjectDisposedException)
                    {
                    }
                }
            }
            finally
            {
                Drain();
                _available.Dispose();
            }
        }

        private bool ShouldDiscard(PooledSocket socket)
        {
            if (!socket.IsAlive)
            {
                return true;
            }

            if (_connectionIdleTimeout > TimeSpan.Zero &&
                socket.LastUsed < DateTime.UtcNow.Subtract(_connectionIdleTimeout))
            {
                if (_logger.IsEnabled(LogLevel.Information))
                {
                    _logger.LogInformation("Connection idle timeout {idleTimeout} reached.", _connectionIdleTimeout);
                }

                return true;
            }

            return false;
        }

        private void Discard(PooledSocket socket)
        {
            try
            {
                socket.Destroy();
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"Failed to destroy {nameof(PooledSocket)}");
            }

            ReleaseReservedSlot();
        }

        private void Drain()
        {
            foreach (var q in _stripes)
            {
                while (q.TryDequeue(out var socket))
                {
                    try
                    {
                        socket.Destroy();
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, $"failed to destroy {nameof(PooledSocket)}");
                    }
                }
            }
        }
    }
}
