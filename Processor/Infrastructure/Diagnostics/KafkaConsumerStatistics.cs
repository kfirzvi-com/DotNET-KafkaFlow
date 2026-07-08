using System.Diagnostics.Metrics;
using System.Text.Json;
using Microsoft.Extensions.Logging;

namespace Processor.Infrastructure.Diagnostics;

/// <summary>
/// Bridges librdkafka's statistics JSON (delivered via KafkaFlow's <c>WithStatisticsHandler</c>) into
/// OpenTelemetry instruments, since KafkaFlow itself exports only traces. Surfaces the fields that
/// reveal what the consumer is doing: lag, assignment, rebalances, fetch-queue depth, broker RTT,
/// and rx/tx message totals.
/// </summary>
public class KafkaConsumerStatistics
{
    public const string MeterName = "KafkaFlowProcessor.Kafka";

    private static readonly Meter Meter = new(MeterName, "1.0.0");

    private readonly ILogger<KafkaConsumerStatistics>? _logger;

    private long _consumerLag;
    private long _assignmentPartitions;
    private long _rebalances;
    private long _fetchqMessages;
    private long _rxMessages;
    private long _txMessages;
    private double _brokerRttMsAvg;
    private double _brokerRttMsMax;

    // Current parsed values (exposed for diagnostics/tests).
    public long ConsumerLag => Interlocked.Read(ref _consumerLag);
    public long AssignmentPartitions => Interlocked.Read(ref _assignmentPartitions);
    public long Rebalances => Interlocked.Read(ref _rebalances);
    public long FetchqMessages => Interlocked.Read(ref _fetchqMessages);
    public long RxMessages => Interlocked.Read(ref _rxMessages);
    public long TxMessages => Interlocked.Read(ref _txMessages);
    public double BrokerRttMsAvg => Volatile.Read(ref _brokerRttMsAvg);
    public double BrokerRttMsMax => Volatile.Read(ref _brokerRttMsMax);

    public KafkaConsumerStatistics(ILogger<KafkaConsumerStatistics>? logger = null)
    {
        _logger = logger;

        Meter.CreateObservableGauge("kafka_consumer_lag", () => Interlocked.Read(ref _consumerLag),
            unit: null, "Total consumer lag across assigned partitions (messages)");
        Meter.CreateObservableGauge("kafka_consumer_assignment_partitions", () => Interlocked.Read(ref _assignmentPartitions),
            unit: null, "Number of partitions currently assigned to this consumer");
        Meter.CreateObservableCounter("kafka_consumer_rebalances", () => Interlocked.Read(ref _rebalances),
            unit: null, "Cumulative consumer group rebalance count");
        Meter.CreateObservableGauge("kafka_consumer_fetchq_messages", () => Interlocked.Read(ref _fetchqMessages),
            unit: null, "Messages sitting in the client fetch queues");
        Meter.CreateObservableCounter("kafka_consumer_rx_messages", () => Interlocked.Read(ref _rxMessages),
            unit: null, "Cumulative messages consumed from brokers");
        Meter.CreateObservableCounter("kafka_producer_tx_messages", () => Interlocked.Read(ref _txMessages),
            unit: null, "Cumulative messages transmitted to brokers");
        Meter.CreateObservableGauge("kafka_broker_rtt_avg", () => Volatile.Read(ref _brokerRttMsAvg),
            "ms", "Average broker round-trip time across brokers");
        Meter.CreateObservableGauge("kafka_broker_rtt_max", () => Volatile.Read(ref _brokerRttMsMax),
            "ms", "Max broker round-trip time across brokers");
    }

    /// <summary>Parses one librdkafka statistics JSON document and updates the instruments' backing values.</summary>
    public void Handle(string statisticsJson)
    {
        try
        {
            using var doc = JsonDocument.Parse(statisticsJson);
            var root = doc.RootElement;

            if (root.TryGetProperty("rxmsgs", out var rx)) Interlocked.Exchange(ref _rxMessages, rx.GetInt64());
            if (root.TryGetProperty("txmsgs", out var tx)) Interlocked.Exchange(ref _txMessages, tx.GetInt64());

            if (root.TryGetProperty("cgrp", out var cgrp))
            {
                if (cgrp.TryGetProperty("rebalance_cnt", out var rb)) Interlocked.Exchange(ref _rebalances, rb.GetInt64());
                if (cgrp.TryGetProperty("assignment_size", out var asz)) Interlocked.Exchange(ref _assignmentPartitions, asz.GetInt64());
            }

            long lag = 0, fetchq = 0;
            if (root.TryGetProperty("topics", out var topics))
            {
                foreach (var topic in topics.EnumerateObject())
                {
                    if (!topic.Value.TryGetProperty("partitions", out var partitions)) continue;
                    foreach (var partition in partitions.EnumerateObject())
                    {
                        if (partition.Name == "-1") continue; // internal unassigned partition
                        if (partition.Value.TryGetProperty("consumer_lag", out var cl))
                        {
                            var v = cl.GetInt64();
                            if (v > 0) lag += v;
                        }
                        if (partition.Value.TryGetProperty("fetchq_cnt", out var fq)) fetchq += fq.GetInt64();
                    }
                }
            }
            Interlocked.Exchange(ref _consumerLag, lag);
            Interlocked.Exchange(ref _fetchqMessages, fetchq);

            double rttSum = 0, rttMax = 0; int rttCount = 0;
            if (root.TryGetProperty("brokers", out var brokers))
            {
                foreach (var broker in brokers.EnumerateObject())
                {
                    // Skip bootstrap/unconnected brokers with no RTT samples.
                    if (broker.Value.TryGetProperty("rtt", out var rtt) &&
                        rtt.TryGetProperty("avg", out var avg))
                    {
                        var micros = avg.GetDouble();
                        if (micros <= 0) continue;
                        var ms = micros / 1000.0;
                        rttSum += ms;
                        rttMax = Math.Max(rttMax, ms);
                        rttCount++;
                    }
                }
            }
            Volatile.Write(ref _brokerRttMsAvg, rttCount > 0 ? rttSum / rttCount : 0);
            Volatile.Write(ref _brokerRttMsMax, rttMax);
        }
        catch (Exception ex)
        {
            _logger?.LogWarning(ex, "Failed to parse Kafka statistics JSON");
        }
    }
}
