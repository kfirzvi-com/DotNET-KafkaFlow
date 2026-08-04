using System.Diagnostics.Metrics;

namespace Processor.Core.Diagnostics;

/// <summary>
/// Process-wide instruments. Every message-outcome counter carries a <c>domain</c> tag so a single
/// dashboard can compare the per-domain deployments that all run this same code.
/// </summary>
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

    private static readonly Counter<long> DataStoreOperations =
        Meter.CreateCounter<long>("datastore_operations", unit: null, "Total data store operations by operation and status");

    private static readonly Histogram<double> DataStoreOperationDuration =
        Meter.CreateHistogram<double>("datastore_operation_duration", "ms", "Data store round-trip duration in milliseconds");

    private static readonly Counter<long> SettingsLoadFailures =
        Meter.CreateCounter<long>("datatype_settings_load_failures", unit: null, "Total failures loading data-type settings (stale snapshot kept)");

    private static readonly Counter<long> SettingsReloads =
        Meter.CreateCounter<long>("datatype_settings_reloads", unit: null, "Total successful data-type settings reloads");

    // Snapshot state for observable gauges (updated on each successful load).
    private static long _snapshotSize;
    private static long _lastLoadTick = -1;

    static ProcessorMetrics()
    {
        Meter.CreateObservableGauge("datatype_settings_snapshot_size",
            () => Interlocked.Read(ref _snapshotSize),
            unit: null, "Number of data-type settings currently held in the in-memory snapshot");

        Meter.CreateObservableGauge("datatype_settings_since_load",
            () =>
            {
                var last = Interlocked.Read(ref _lastLoadTick);
                return last < 0 ? -1 : (Environment.TickCount64 - last) / 1000.0;
            },
            "s", "Seconds since the last successful settings load (-1 if never loaded)");
    }

    private static KeyValuePair<string, object?> DomainTag(string domain) => new("domain", domain);

    public static void RecordProcessed(string domain) => MessagesProcessed.Add(1, DomainTag(domain));

    public static void RecordDeadLettered(string domain) => MessagesDeadLettered.Add(1, DomainTag(domain));

    public static void RecordDropped(string domain) => MessagesDropped.Add(1, DomainTag(domain));

    /// <summary>Records a message filtered out because its data type is inactive or unknown.</summary>
    public static void RecordFiltered(string domain, string reason) =>
        MessagesFiltered.Add(1, DomainTag(domain), new KeyValuePair<string, object?>("reason", reason));

    public static void RecordProcessingDuration(string domain, double durationMs) =>
        MessageProcessingDuration.Record(durationMs, DomainTag(domain));

    /// <summary>Records a data store round-trip (count + latency), tagged by operation and status (ok/error).</summary>
    public static void RecordDataStoreOperation(string operation, string status, double durationMs)
    {
        var operationTag = new KeyValuePair<string, object?>("operation", operation);
        var statusTag = new KeyValuePair<string, object?>("status", status);

        DataStoreOperations.Add(1, operationTag, statusTag);
        DataStoreOperationDuration.Record(durationMs, operationTag, statusTag);
    }

    /// <summary>Records a failed settings load (a stale snapshot is kept).</summary>
    public static void RecordSettingsLoadFailure() => SettingsLoadFailures.Add(1);

    /// <summary>Records a successful settings reload and updates snapshot-state gauges.</summary>
    public static void RecordSettingsLoadSuccess(int snapshotSize)
    {
        SettingsReloads.Add(1);
        Interlocked.Exchange(ref _snapshotSize, snapshotSize);
        Interlocked.Exchange(ref _lastLoadTick, Environment.TickCount64);
    }
}
