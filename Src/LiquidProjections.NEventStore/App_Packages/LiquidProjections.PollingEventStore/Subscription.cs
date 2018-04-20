using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using LiquidProjections.Abstractions;

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
        private readonly LogMessage logger;

        public Subscription(PollingEventStoreAdapter eventStoreAdapter, long lastProcessedCheckpoint,
            Subscriber subscriber, string subscriptionId, LogMessage logger)
        {
            this.eventStoreAdapter = eventStoreAdapter;
            this.lastProcessedCheckpoint = lastProcessedCheckpoint;
            this.subscriber = subscriber;
            this.logger = logger;
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
#if LIQUIDPROJECTIONS_DIAGNOSTICS
                logger(() => $"Subscription {Id} has been started.");
#endif

                var info = new SubscriptionInfo
                {
                    Id = Id,
                    Subscription = this,
                    CancellationToken = cancellationTokenSource.Token
                };

                Task = Task.Run(async () =>
                        {
                            try
                            {
                                await RunAsync(info).ConfigureAwait(false);
                            }
                            catch (OperationCanceledException)
                            {
                                // Do nothing.
                            }
                            catch (Exception exception)
                            {
                                logger(() => 
                                    "NEventStore polling task has failed. Event subscription has been cancelled: " +
                                    exception);
                            }
                        },
                        CancellationToken.None);
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
                Page page = await TryGetNextPage(lastProcessedCheckpoint).ConfigureAwait(false);
                if (page != null)
                {
                    var transactions = page.Transactions;

                    if (firstRequestAfterSubscribing)
                    {
                        if (!transactions.Any())
                        {
                            await subscriber.NoSuchCheckpoint(info).ConfigureAwait(false);
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

#if LIQUIDPROJECTIONS_DIAGNOSTICS
                        logger(() =>
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
#if LIQUIDPROJECTIONS_DIAGNOSTICS
                logger(() =>
                    $"Subscription {Id} is requesting a page after checkpoint {checkpoint}.");
#endif

                page = await eventStoreAdapter.GetNextPage(checkpoint, Id)
                    .WithWaitCancellation(cancellationTokenSource.Token)
                    .ConfigureAwait(false);

#if LIQUIDPROJECTIONS_DIAGNOSTICS
                logger(() =>
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

#if LIQUIDPROJECTIONS_DIAGNOSTICS
                    logger(() => $"Subscription {Id} is being stopped.");
#endif

                    if (cancellationTokenSource != null)
                    {
                        try
                        {
                            cancellationTokenSource.Cancel();
                        }
                        catch (AggregateException)
                        {
                            // Ignore.
                        }
                    }

                    lock (eventStoreAdapter.subscriptionLock)
                    {
                        eventStoreAdapter.subscriptions.Remove(this);
                    }
                    
                    if (Task == null)
                    {
                        FinishDisposing();
                    }
                    else
                    {
                        // Wait for the task asynchronously.
                        Task.ContinueWith(_ => FinishDisposing());
                    }
                }
            }
        }

        private void FinishDisposing()
        {
            cancellationTokenSource?.Dispose();
            
#if LIQUIDPROJECTIONS_DIAGNOSTICS
            logger(() => $"Subscription {Id} has been stopped.");
#endif
        }
    }
}