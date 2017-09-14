using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using LiquidProjections.Abstractions;
using LiquidProjections.NEventStore.Logging;

namespace LiquidProjections.PollingEventStore
{
    internal sealed class Subscription : IDisposable
    {
        private readonly PollingEventStoreAdapter eventStoreAdapter;
        private CancellationTokenSource cancellationTokenSource;
        private readonly object syncRoot = new object();
        private bool isDisposed;
        private long lastProcessedCheckpoint;
        private readonly Subscriber subscriber;

        public Subscription(PollingEventStoreAdapter eventStoreAdapter, long lastProcessedCheckpoint,
            Subscriber subscriber, string subscriptionId)
        {
            this.eventStoreAdapter = eventStoreAdapter;
            this.lastProcessedCheckpoint = lastProcessedCheckpoint;
            this.subscriber = subscriber;
            Id = subscriptionId;
        }

        public Task Task { get; private set; }

        public string Id { get; }

        public void Start()
        {
            if (Task != null)
            {
                throw new InvalidOperationException("Already started.");
            }

            lock (syncRoot)
            {
                if (isDisposed)
                {
                    throw new ObjectDisposedException(nameof(Subscription));
                }

                cancellationTokenSource = new CancellationTokenSource();
#if DEBUG
                LogProvider.GetLogger(typeof(Subscription)).Debug(() => $"Subscription {Id} has been started.");
#endif

                var info = new SubscriptionInfo
                {
                    Id = Id,
                    Subscription = this,
                    CancellationToken = cancellationTokenSource.Token
                };

                Task = Task.Factory.StartNew(async () =>
                        {
                            try
                            {
                                await RunAsync(info);
                            }
                            catch (OperationCanceledException)
                            {
                                // Do nothing.
                            }
                            catch (Exception exception)
                            {
                                LogProvider.GetLogger(typeof(Subscription)).FatalException(
                                    "NEventStore polling task has failed. Event subscription has been cancelled.",
                                    exception);
                            }
                        },
                        cancellationTokenSource.Token,
                        TaskCreationOptions.DenyChildAttach | TaskCreationOptions.LongRunning,
                        TaskScheduler.Default)
                    .Unwrap();
            }
        }

        private async Task RunAsync(SubscriptionInfo info)
        {
            const int offsetToDetectAheadSubscriber = 1;
            long actualRequestedLastCheckpoint = lastProcessedCheckpoint;
            lastProcessedCheckpoint = lastProcessedCheckpoint > 0 ? lastProcessedCheckpoint - offsetToDetectAheadSubscriber : 0;
            bool firstRequestAfterSubscribing = true;

            while (!cancellationTokenSource.IsCancellationRequested)
            {
                Page page = await TryGetNextPage(lastProcessedCheckpoint);
                if (page != null)
                {
                    var transactions = page.Transactions;

                    if (firstRequestAfterSubscribing)
                    {
                        if (!transactions.Any())
                        {
                            await subscriber.NoSuchCheckpoint(info);
                        }
                        else
                        {
                            transactions = transactions
                                .Where(t => t.Checkpoint > actualRequestedLastCheckpoint)
                                .ToReadOnlyList();
                        }

                        firstRequestAfterSubscribing = false;
                    }

                    if (transactions.Count > 0)
                    {
                        await subscriber.HandleTransactions(transactions, info).ConfigureAwait(false);

#if DEBUG
                        LogProvider.GetLogger(typeof(Subscription)).Debug(() =>
                            $"Subscription {Id} has processed a page of size {page.Transactions.Count} " +
                            $"from checkpoint {page.Transactions.First().Checkpoint} " +
                            $"to checkpoint {page.Transactions.Last().Checkpoint}.");
#endif

                        lastProcessedCheckpoint = page.LastCheckpoint;
                    }
                }
            }
        }

        private async Task<Page> TryGetNextPage(long checkpoint)
        {
            Page page = null;

            try
            {
#if DEBUG
                LogProvider.GetLogger(typeof(Subscription)).Debug(() =>
                    $"Subscription {Id} is requesting a page after checkpoint {checkpoint}.");
#endif

                page = await eventStoreAdapter.GetNextPage(checkpoint, Id)
                    .WithWaitCancellation(cancellationTokenSource.Token)
                    .ConfigureAwait(false);

#if DEBUG
                LogProvider.GetLogger(typeof(Subscription)).Debug(() =>
                    $"Subscription {Id} has got a page of size {page.Transactions.Count} " +
                    $"from checkpoint {page.Transactions.First().Checkpoint} " +
                    $"to checkpoint {page.Transactions.Last().Checkpoint}.");
#endif
            }
            catch
            {
                // Just continue the next iteration after a small pause
            }

            return page;
        }

        public void Complete()
        {
            Dispose();
        }

        public void Dispose()
        {
            lock (syncRoot)
            {
                if (!isDisposed)
                {
                    isDisposed = true;

                    // Wait for the task asynchronously.
                    Task.Run(() =>
                    {
                        if (cancellationTokenSource != null)
                        {
#if DEBUG
                            LogProvider.GetLogger(typeof(Subscription)).Debug(() => $"Subscription {Id} is being stopped.");
#endif

                            if (!cancellationTokenSource.IsCancellationRequested)
                            {
                                cancellationTokenSource.Cancel();
                            }

                            try
                            {
                                Task?.Wait();
                            }
                            catch (AggregateException)
                            {
                                // Ignore.
                            }

                            cancellationTokenSource.Dispose();
                        }

                        lock (eventStoreAdapter.subscriptionLock)
                        {
                            eventStoreAdapter.subscriptions.Remove(this);
                        }

#if DEBUG
                        LogProvider.GetLogger(typeof(Subscription)).Debug(() => $"Subscription {Id} has been stopped.");
#endif
                    });
                }
            }
        }
    }
}