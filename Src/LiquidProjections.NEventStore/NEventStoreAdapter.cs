using System;
using System.Collections.Generic;
using System.Linq;
using LiquidProjections.Abstractions;
using NEventStore;
using NEventStore.Persistence;
using LiquidProjections.PollingEventStore;

namespace LiquidProjections.NEventStore
{
    /// <summary>
    /// An adapter to NEventStore's <see cref="IPersistStreams"/> that efficiently supports multiple concurrent subscribers
    /// each interested in a different checkpoint, without hitting the event store concurrently. 
    /// </summary>
    public class NEventStoreAdapter : IDisposable
    {
        private readonly PollingEventStoreAdapter pollingAdapter;

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
            pollingAdapter = new PollingEventStoreAdapter(new StreamPersisterAdapter(eventStore), cacheSize, pollInterval, maxPageSize, getUtcNow);
        }

        public IDisposable Subscribe(long? lastProcessedCheckpoint, Subscriber subscriber, string subscriptionId)
        {
            return pollingAdapter.Subscribe(lastProcessedCheckpoint, subscriber, subscriptionId);
        }
        
        public void Dispose()
        {
            pollingAdapter?.Dispose();
        }
    }

    internal class StreamPersisterAdapter : IPassiveEventStore
    {
        private readonly IPersistStreams streamPersister;

        public StreamPersisterAdapter(IPersistStreams streamPersister)
        {
            this.streamPersister = streamPersister;
        }

        public IEnumerable<Transaction> GetFrom(long? checkpoint)
        {
            return streamPersister
                .GetFrom((!checkpoint.HasValue || checkpoint == 0) ? null : checkpoint.ToString())
                .Select(ToTransaction);
        }

        private Transaction ToTransaction(ICommit commit)
        {
            return new Transaction
            {
                Id = commit.CommitId.ToString(),
                StreamId = commit.StreamId,
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
    }
}