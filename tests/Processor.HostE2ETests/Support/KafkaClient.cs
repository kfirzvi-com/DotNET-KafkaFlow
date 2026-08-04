using System.Text;
using System.Text.Json;
using Confluent.Kafka;

namespace Processor.HostE2ETests.Support;

/// <summary>
/// Produces to and consumes from the real broker. Messages are serialized with System.Text.Json
/// defaults (PascalCase), matching what KafkaFlow's <c>JsonCoreDeserializer</c> expects on the wire.
/// </summary>
public sealed class KafkaClient : IDisposable
{
    private readonly string _bootstrapServers;
    private readonly IProducer<string?, string> _producer;

    public KafkaClient(string bootstrapServers)
    {
        _bootstrapServers = bootstrapServers;
        _producer = new ProducerBuilder<string?, string>(new ProducerConfig
        {
            BootstrapServers = bootstrapServers,
            Acks = Acks.All
        }).Build();
    }

    /// <summary>
    /// Publishes a message. The data type id travels as the Kafka key by default (the processor falls
    /// back to the key when the header is absent), or as the header when asked.
    /// </summary>
    public async Task ProduceAsync<T>(
        string topic, T message, string? dataTypeId, bool useHeader = false)
    {
        var kafkaMessage = new Message<string?, string>
        {
            Key = useHeader ? null : dataTypeId,
            Value = JsonSerializer.Serialize(message)
        };

        if (useHeader && dataTypeId is not null)
        {
            kafkaMessage.Headers = new Headers
            {
                { "data-type-id", Encoding.UTF8.GetBytes(dataTypeId) }
            };
        }

        await _producer.ProduceAsync(topic, kafkaMessage);
        _producer.Flush(TimeSpan.FromSeconds(10));
    }

    /// <summary>
    /// Reads from the beginning of a topic until <paramref name="count"/> messages arrive or the timeout
    /// elapses. Returns what it got, so a caller can assert on "fewer than expected" too.
    /// </summary>
    public List<T> Consume<T>(string topic, int count, TimeSpan timeout)
    {
        using var consumer = new ConsumerBuilder<string?, string>(new ConsumerConfig
        {
            BootstrapServers = _bootstrapServers,
            // A fresh group per call so every read starts at the beginning of the topic.
            GroupId = $"assert-{Guid.NewGuid():N}",
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false
        }).Build();

        consumer.Subscribe(topic);

        var options = new JsonSerializerOptions { PropertyNameCaseInsensitive = true };
        var results = new List<T>();
        var deadline = DateTime.UtcNow + timeout;

        while (results.Count < count && DateTime.UtcNow < deadline)
        {
            var result = consumer.Consume(TimeSpan.FromMilliseconds(500));
            if (result?.Message?.Value is null)
            {
                continue;
            }

            var parsed = JsonSerializer.Deserialize<T>(result.Message.Value, options);
            if (parsed is not null)
            {
                results.Add(parsed);
            }
        }

        consumer.Close();
        return results;
    }

    /// <summary>
    /// Asserts a topic stays empty for <paramref name="quietPeriod"/>. Used for the drop and filter
    /// outcomes, where the contract is that nothing is produced at all.
    /// </summary>
    public void AssertEmpty(string topic, TimeSpan quietPeriod)
    {
        var messages = Consume<JsonElement>(topic, count: 1, timeout: quietPeriod);
        Assert.Empty(messages);
    }

    public void Dispose() => _producer.Dispose();
}
