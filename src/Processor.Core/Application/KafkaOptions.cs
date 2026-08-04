namespace Processor.Core.Application;

/// <summary>
/// Kafka wiring for this deployment. Topics and the consumer group are per-domain in practice — each
/// domain's deployment points at its own topics via configuration, not via code.
/// </summary>
public class KafkaOptions
{
    public const string SectionName = "Kafka";

    /// <summary>
    /// Broker addresses. Deliberately empty by default: the configuration binder <em>appends</em> to a
    /// non-empty collection rather than replacing it, so a default like <c>localhost:9092</c> would
    /// survive into a production deployment's broker list. Startup rejects an empty value instead.
    /// </summary>
    public string[] Brokers { get; set; } = Array.Empty<string>();

    public string ConsumerGroupId { get; set; } = "message-processor-group";

    public string InputTopic { get; set; } = "input-topic";

    public string OutputTopic { get; set; } = "output-topic";

    public string DeadLetterTopic { get; set; } = "dead-letter-topic";

    /// <summary>Worker loops processing messages in parallel. Rule of thumb: ≈ the pod's CPU allotment.</summary>
    public int WorkersCount { get; set; } = 10;

    /// <summary>Per-worker bounded prefetch channel. Total in-flight ≈ WorkersCount × BufferSize.</summary>
    public int BufferSize { get; set; } = 100;

    /// <summary>librdkafka statistics emission interval, bridged into OTel metrics.</summary>
    public int StatisticsIntervalMs { get; set; } = 5000;

    /// <summary>
    /// Where a consumer group with no committed offset starts: <c>earliest</c> (the default here) or
    /// <c>latest</c>.
    /// <para>
    /// This deliberately overrides KafkaFlow's own <c>latest</c> default. The processor must consume
    /// every message it can: with <c>latest</c> a new deployment silently skips the existing backlog,
    /// and anything produced before the group's first partition assignment completes is lost outright.
    /// Nothing here depends on starting at the head of the topic.
    /// </para>
    /// </summary>
    public string AutoOffsetReset { get; set; } = "earliest";

    /// <summary>Parses <see cref="AutoOffsetReset"/>, rejecting anything unrecognised.</summary>
    public KafkaFlow.AutoOffsetReset ResolveAutoOffsetReset() => AutoOffsetReset?.Trim().ToLowerInvariant() switch
    {
        "latest" => KafkaFlow.AutoOffsetReset.Latest,
        "earliest" or null or "" => KafkaFlow.AutoOffsetReset.Earliest,
        _ => throw new InvalidOperationException(
            $"Unknown {SectionName}:{nameof(AutoOffsetReset)} '{AutoOffsetReset}'. Expected 'earliest' or 'latest'.")
    };
}
