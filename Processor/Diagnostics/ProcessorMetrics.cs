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

    private static readonly Histogram<double> MessageProcessingDuration =
        Meter.CreateHistogram<double>("messages_processing_duration", "ms", "Message processing duration in milliseconds");

    public static void RecordProcessed() => MessagesProcessed.Add(1);
    public static void RecordDeadLettered() => MessagesDeadLettered.Add(1);
    public static void RecordDropped() => MessagesDropped.Add(1);
    public static void RecordProcessingDuration(double durationMs) => MessageProcessingDuration.Record(durationMs);
}
