using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using LiquidProjections.Abstractions;
using NEventStore;
using NEventStore.Persistence;
using LiquidProjections.NEventStore.Logging;

namespace LiquidProjections.NEventStore
{
    /// <summary>
    /// An adapter to NEventStore's <see cref="IPersistStreams"/> that efficiently supports multiple concurrent subscribers
    /// each interested in a different checkpoint, without hitting the event store concurrently. 
    /// </summary>
    public class NEventStoreAdapter : IDisposable
    {
        private readonly TimeSpan pollInterval;
        private readonly int maxPageSize;
        private readonly Func<DateTime> getUtcNow;
        private readonly IPersistStreams eventStore;
        internal readonly HashSet<Subscription> subscriptions = new HashSet<Subscription>();
        private volatile bool isDisposed;
        internal readonly object subscriptionLock = new object();
        private Task<Page> currentLoader;

        /// <summary>
        /// Stores cached transactions by the checkpoint of their previous transaction.
        /// </summary>
        private readonly LruCache<long, Transaction> transactionCacheByPreviousCheckpoint;

        private readonly CancellationTokenSource cancellationTokenSource = new CancellationTokenSource();
        private CheckpointRequestTimestamp lastSuccessfulPollingRequestWithoutResults;

        /// <summary>
        /// Creates an adapter that observes an implementation of <see cref="IPersistStreams"/> and efficiently handles
        /// multiple subscribers.
        /// </summary>
        /// <param name="eventStore">
        /// The persistency implementation that the NEventStore is configured with.
        /// </param>
        /// <param name="cacheSize">
        /// The size of the LRU cache that will hold transactions already loaded from the event store. The larger the cache, 
        /// the higher the chance multiple subscribers can reuse the same transactions without hitting the underlying event store.
        /// Set to <c>0</c> to disable the cache alltogether.
        /// </param>
        /// <param name="pollInterval">
        /// The amount of time to wait before polling again after the event store has not yielded any transactions anymore.
        /// </param>
        /// <param name="maxPageSize">
        /// The size of the page of transactions the adapter should load from the event store for every query.
        /// </param>
        /// <param name="getUtcNow">
        /// Provides the current date and time in UTC.
        /// </param>
        public NEventStoreAdapter(IPersistStreams eventStore, int cacheSize, TimeSpan pollInterval, int maxPageSize,
            Func<DateTime> getUtcNow)
        {
            this.eventStore = eventStore;
            this.pollInterval = pollInterval;
            this.maxPageSize = maxPageSize;
            this.getUtcNow = getUtcNow;

            if (cacheSize > 0)
            {
                transactionCacheByPreviousCheckpoint = new LruCache<long, Transaction>(cacheSize);
            }
        }

        public IDisposable Subscribe(long? lastProcessedCheckpoint, Subscriber subscriber, string subscriptionId)
        {
            if (subscriber == null)
            {
                throw new ArgumentNullException(nameof(subscriber));
            }

            Subscription subscription;

            lock (subscriptionLock)
            {
                if (isDisposed)
                {
                    throw new ObjectDisposedException(typeof(NEventStoreAdapter).FullName);
                }

                subscription = new Subscription(this, lastProcessedCheckpoint ?? 0, subscriber, subscriptionId);
                subscriptions.Add(subscription);
            }

            subscription.Start();
            return subscription;
        }

        internal async Task<Page> GetNextPage(long lastProcessedCheckpoint, string subscriptionId)
        {
            if (isDisposed)
            {
                throw new ObjectDisposedException(typeof(NEventStoreAdapter).FullName);
            }

            Page pageFromCache = TryGetNextPageFromCache(lastProcessedCheckpoint, subscriptionId);
            if (pageFromCache.Transactions.Count > 0)
            {
                return pageFromCache;
            }

            Page loadedPage = await LoadNextPageSequentially(lastProcessedCheckpoint, subscriptionId).ConfigureAwait(false);
            if (loadedPage.Transactions.Count == maxPageSize)
            {
                StartPreloadingNextPage(loadedPage.LastCheckpoint, subscriptionId);
            }

            return loadedPage;
        }

        private Page TryGetNextPageFromCache(long previousCheckpoint, string subscriptionId)
        {
            Transaction cachedNextTransaction;

            if ((transactionCacheByPreviousCheckpoint != null) && transactionCacheByPreviousCheckpoint.TryGet(previousCheckpoint, out cachedNextTransaction))
            {
                var resultPage = new List<Transaction>(maxPageSize) { cachedNextTransaction };

                while (resultPage.Count < maxPageSize)
                {
                    long lastCheckpoint = cachedNextTransaction.Checkpoint;

                    if (transactionCacheByPreviousCheckpoint.TryGet(lastCheckpoint, out cachedNextTransaction))
                    {
                        resultPage.Add(cachedNextTransaction);
                    }
                    else
                    {
                        StartPreloadingNextPage(lastCheckpoint, subscriptionId);
                        break;
                    }
                }

#if DEBUG
                LogProvider.GetCurrentClassLogger().Debug(() =>
                    $"Subscription {subscriptionId} has found a page of size {resultPage.Count} " +
                    $"from checkpoint {resultPage.First().Checkpoint} " +
                    $"to checkpoint {resultPage.Last().Checkpoint} in the cache.");
#endif

                return new Page(previousCheckpoint, resultPage);
            }

#if DEBUG
            LogProvider.GetCurrentClassLogger().Debug(() =>
                $"Subscription {subscriptionId} has not found the next transaction in the cache.");
#endif

            return new Page(previousCheckpoint, new Transaction[0]);
        }

        private void StartPreloadingNextPage(long previousCheckpoint, string subscriptionId)
        {
#if DEBUG
            LogProvider.GetCurrentClassLogger().Debug(() =>
                $"Subscription {subscriptionId} has started preloading transactions " +
                $"after checkpoint {previousCheckpoint}.");
#endif

            // Ignore result.
            Task _ = LoadNextPageSequentially(previousCheckpoint, subscriptionId);
        }

        private async Task<Page> LoadNextPageSequentially(long previousCheckpoint, string subscriptionId)
        {
            while (true)
            {
                cancellationTokenSource.Token.ThrowIfCancellationRequested();

                if (isDisposed)
                {
                    return new Page(previousCheckpoint, new Transaction[0]);
                }

                CheckpointRequestTimestamp effectiveLastExistingCheckpointRequest =
                    Volatile.Read(ref lastSuccessfulPollingRequestWithoutResults);

                if ((effectiveLastExistingCheckpointRequest != null) &&
                    (effectiveLastExistingCheckpointRequest.PreviousCheckpoint == previousCheckpoint))
                {
                    TimeSpan timeAfterPreviousRequest = getUtcNow() - effectiveLastExistingCheckpointRequest.DateTimeUtc;
                    if (timeAfterPreviousRequest < pollInterval)
                    {
                        TimeSpan delay = pollInterval - timeAfterPreviousRequest;

#if DEBUG
                        LogProvider.GetCurrentClassLogger().Debug(() =>
                            $"Subscription {subscriptionId} is waiting " +
                            $"for {delay} before checking for new transactions.");
#endif

                        await Task.Delay(delay).ConfigureAwait(false);
                    }
                }

                Page candidatePage = await TryLoadNextPageSequentiallyOrWaitForCurrentLoadingToFinish(previousCheckpoint,
                        subscriptionId)
                    .ConfigureAwait(false);

                if (candidatePage.PreviousCheckpoint == previousCheckpoint)
                {
                    return candidatePage;
                }
            }
        }

        private Task<Page> TryLoadNextPageSequentiallyOrWaitForCurrentLoadingToFinish(long previousCheckpoint,
            string subscriptionId)
        {
            if (isDisposed)
            {
                return Task.FromResult(new Page(previousCheckpoint, new Transaction[0]));
            }

            TaskCompletionSource<Page> taskCompletionSource = null;
            bool isTaskOwner = false;
            Task<Page> loader = Volatile.Read(ref currentLoader);

            try
            {
                if (loader == null)
                {
                    taskCompletionSource = new TaskCompletionSource<Page>();
                    Task<Page> oldLoader = Interlocked.CompareExchange(ref currentLoader, taskCompletionSource.Task, null);
                    isTaskOwner = oldLoader == null;
                    loader = isTaskOwner ? taskCompletionSource.Task : oldLoader;
                }

                return loader;
            }
            finally
            {
                if (isTaskOwner)
                {
#if DEBUG
                    LogProvider.GetCurrentClassLogger()
                        .Debug(() => $"Subscription {subscriptionId} created a loader {loader.Id} " +
                                     $"for a page after checkpoint {previousCheckpoint}.");
#endif

                    // Ignore result.
                    Task _ = TryLoadNextPageAndMakeLoaderComplete(previousCheckpoint, taskCompletionSource, subscriptionId);
                }
                else
                {
#if DEBUG
                    LogProvider.GetCurrentClassLogger()
                        .Debug(() => $"Subscription {subscriptionId} is waiting for loader {loader.Id}.");
#endif
                }
            }
        }

        private async Task TryLoadNextPageAndMakeLoaderComplete(long previousCheckpoint,
            TaskCompletionSource<Page> loaderCompletionSource, string subscriptionId)
        {
            Page nextPage;

            try
            {
                try
                {
                    nextPage = await TryLoadNextPage(previousCheckpoint, subscriptionId).ConfigureAwait(false);
                }
                finally
                {
#if DEBUG
                    LogProvider.GetCurrentClassLogger().Debug(() =>
                        $"Loader for subscription {subscriptionId} is no longer the current one.");
#endif
                    Volatile.Write(ref currentLoader, null);
                }
            }
            catch (Exception exception)
            {
#if DEBUG
                LogProvider.GetCurrentClassLogger().DebugException(
                    $"Loader for subscription {subscriptionId} has failed.",
                    exception);
#endif

                loaderCompletionSource.SetException(exception);
                return;
            }

#if DEBUG
            LogProvider.GetCurrentClassLogger().Debug(() =>
                $"Loader for subscription {subscriptionId} has completed.");
#endif
            loaderCompletionSource.SetResult(nextPage);
        }

        private async Task<Page> TryLoadNextPage(long previousCheckpoint, string subscriptionId)
        {
            // Maybe it's just loaded to cache.
            try
            {
                Page cachedPage = TryGetNextPageFromCache(previousCheckpoint, subscriptionId);
                if (cachedPage.Transactions.Count > 0)
                {
#if DEBUG
                    LogProvider.GetCurrentClassLogger()
                        .Debug(() =>
                            $"Loader for subscription {subscriptionId} has found a page in the cache.");
#endif
                    return cachedPage;
                }
            }
            catch (Exception exception)
            {
                LogProvider.GetLogger(typeof(NEventStoreAdapter))
                    .ErrorException(
                        $"Failed getting transactions after checkpoint {previousCheckpoint} from the cache.",
                        exception);
            }

            DateTime timeOfRequestUtc = getUtcNow();
            List<Transaction> transactions;

            try
            {
                transactions = await Task
                    .Run(() => eventStore
                        .GetFrom((previousCheckpoint == 0) ? null : previousCheckpoint.ToString())
                        .Take(maxPageSize)
                        .Select(ToTransaction)
                        .ToList())
                    .ConfigureAwait(false);
            }
            catch (Exception exception)
            {
                LogProvider.GetLogger(typeof(NEventStoreAdapter))
                    .ErrorException(
                        $"Failed loading transactions after checkpoint {previousCheckpoint} from NEventStore",
                        exception);

                throw;
            }

            if (transactions.Count > 0)
            {
#if DEBUG
                LogProvider.GetCurrentClassLogger().Debug(() =>
                    $"Loader for subscription {subscriptionId ?? "without ID"} has loaded {transactions.Count} transactions " +
                    $"from checkpoint {transactions.First().Checkpoint} to checkpoint {transactions.Last().Checkpoint}.");
#endif

                if (transactionCacheByPreviousCheckpoint != null)
                {
                    /* Add to cache in reverse order to prevent other projectors
                        from requesting already loaded transactions which are not added to cache yet. */
                    for (int index = transactions.Count - 1; index > 0; index--)
                    {
                        transactionCacheByPreviousCheckpoint.Set(transactions[index - 1].Checkpoint, transactions[index]);
                    }

                    transactionCacheByPreviousCheckpoint.Set(previousCheckpoint, transactions[0]);

#if DEBUG
                    LogProvider.GetCurrentClassLogger().Debug(() =>
                        $"Loader for subscription {subscriptionId ?? "without ID"} has cached {transactions.Count} transactions " +
                        $"from checkpoint {transactions.First().Checkpoint} to checkpoint {transactions.Last().Checkpoint}.");
#endif
                }
            }
            else
            {
#if DEBUG
                LogProvider.GetCurrentClassLogger().Debug(() =>
                    $"Loader for subscription {subscriptionId} has discovered "+
                    $"that there are no new transactions yet. Next request for the new transactions will be delayed.");
#endif

                Volatile.Write(
                    ref lastSuccessfulPollingRequestWithoutResults,
                    new CheckpointRequestTimestamp(previousCheckpoint, timeOfRequestUtc));
            }

            return new Page(previousCheckpoint, transactions);
        }

        private Transaction ToTransaction(ICommit commit)
        {
            return new Transaction
            {
                Id = commit.CommitId.ToString(),
                StreamId = commit.StreamId,

                // SMELL properly log an exception that we only support numeric based storage engines
                Checkpoint = long.Parse(commit.CheckpointToken),
                TimeStampUtc = commit.CommitStamp,
                Events = new List<EventEnvelope>(commit.Events.Select(@event => new EventEnvelope
                {
                    Body = @event.Body,
                    Headers = @event.Headers
                })),
                Headers = commit.Headers
            };
        }

        public void Dispose()
        {
            lock (subscriptionLock)
            {
                if (!isDisposed)
                {
                    isDisposed = true;

                    cancellationTokenSource.Cancel();

                    foreach (Subscription subscription in subscriptions.ToArray())
                    {
                        subscription.Complete();
                    }

                    Task loaderToWaitFor = Volatile.Read(ref currentLoader);
                    loaderToWaitFor?.Wait();

                    cancellationTokenSource.Dispose();
                    eventStore.Dispose();
                }
            }
        }
    }
}