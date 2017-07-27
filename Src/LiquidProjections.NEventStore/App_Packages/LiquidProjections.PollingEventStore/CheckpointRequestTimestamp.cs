using System;

namespace LiquidProjections.PollingEventStore
{
    internal sealed class CheckpointRequestTimestamp
    {
        public CheckpointRequestTimestamp(long previousCheckpoint, DateTime dateTimeUtc)
        {
            PreviousCheckpoint = previousCheckpoint;
            DateTimeUtc = dateTimeUtc;
        }

        public long PreviousCheckpoint { get; }
        public DateTime DateTimeUtc { get; }
    }
}