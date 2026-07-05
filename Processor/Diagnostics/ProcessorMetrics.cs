using System.Diagnostics.Metrics;

namespace Processor.Diagnostics;

public static class ProcessorMetrics
{
    public const string MeterName = "KafkaFlowProcessor";

    private static readonly Meter Meter = new(MeterName, "1.0.0");

    // No unit is set on the counters: the Prometheus exporter turns a unit into a name
    // suffix, which would produce the redundant "messages_processed_messages_total".
    private static readonly Counter<long> MessagesProcessed =
        Meter.CreateCounter<long>("messages_processed", unit: null, "Total messages successfully processed");

    private static readonly Counter<long> MessagesDeadLettered =
        Meter.CreateCounter<long>("messages_dead_lettered", unit: null, "Total messages sent to dead letter queue");

    private static readonly Counter<long> MessagesDropped =
        Meter.CreateCounter<long>("messages_dropped", unit: null, "Total messages dropped");

    private static readonly Counter<long> MessagesFiltered =
        Meter.CreateCounter<long>("messages_filtered", unit: null, "Total messages filtered out by data-type settings");

    private static readonly Histogram<double> MessageProcessingDuration =
        Meter.CreateHistogram<double>("messages_processing_duration", "ms", "Message processing duration in milliseconds");

    private static readonly Counter<long> RedisOperations =
        Meter.CreateCounter<long>("redis_operations", unit: null, "Total Redis operations by operation and status");

    private static readonly Histogram<double> RedisOperationDuration =
        Meter.CreateHistogram<double>("redis_operation_duration", "ms", "Redis operation round-trip duration in milliseconds");

    public static void RecordProcessed() => MessagesProcessed.Add(1);
    public static void RecordDeadLettered() => MessagesDeadLettered.Add(1);
    public static void RecordDropped() => MessagesDropped.Add(1);

    /// <summary>Records a message filtered out because its data type is inactive or unknown.</summary>
    public static void RecordFiltered(string reason) =>
        MessagesFiltered.Add(1, new KeyValuePair<string, object?>("reason", reason));

    public static void RecordProcessingDuration(double durationMs) => MessageProcessingDuration.Record(durationMs);

    /// <summary>Records a Redis round-trip (count + latency), tagged by operation and status (ok/error).</summary>
    public static void RecordRedisOperation(string operation, string status, double durationMs)
    {
        var operationTag = new KeyValuePair<string, object?>("operation", operation);
        var statusTag = new KeyValuePair<string, object?>("status", status);

        RedisOperations.Add(1, operationTag, statusTag);
        RedisOperationDuration.Record(durationMs, operationTag, statusTag);
    }
}
