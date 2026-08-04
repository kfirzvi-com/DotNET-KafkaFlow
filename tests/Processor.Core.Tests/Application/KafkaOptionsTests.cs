using Processor.Core.Application;

namespace Processor.Core.Tests.Application;

public class KafkaOptionsTests
{
    [Fact]
    public void Brokers_DefaultToEmpty_SoConfigurationReplacesRatherThanAppends()
    {
        // The .NET configuration binder ADDS to a non-empty collection default instead of replacing it,
        // so a seeded default (e.g. localhost) would survive into a production broker list.
        Assert.Empty(new KafkaOptions().Brokers);
    }

    [Fact]
    public void Defaults_MatchTheDocumentedValues()
    {
        var options = new KafkaOptions();

        Assert.Equal("message-processor-group", options.ConsumerGroupId);
        Assert.Equal("input-topic", options.InputTopic);
        Assert.Equal("output-topic", options.OutputTopic);
        Assert.Equal("dead-letter-topic", options.DeadLetterTopic);
        Assert.Equal(10, options.WorkersCount);
        Assert.Equal(100, options.BufferSize);
        Assert.Equal(5000, options.StatisticsIntervalMs);
    }

    [Fact]
    public void ResolveAutoOffsetReset_DefaultsToEarliest_SoNoBacklogIsSkipped()
    {
        // Deliberately not KafkaFlow's own `latest` default: `latest` makes a new deployment skip the
        // existing backlog, and loses anything produced before the first partition assignment lands.
        Assert.Equal(KafkaFlow.AutoOffsetReset.Earliest, new KafkaOptions().ResolveAutoOffsetReset());
    }

    [Theory]
    [InlineData("earliest", KafkaFlow.AutoOffsetReset.Earliest)]
    [InlineData("EARLIEST", KafkaFlow.AutoOffsetReset.Earliest)]
    [InlineData("  earliest ", KafkaFlow.AutoOffsetReset.Earliest)]
    [InlineData("latest", KafkaFlow.AutoOffsetReset.Latest)]
    [InlineData("LATEST", KafkaFlow.AutoOffsetReset.Latest)]
    [InlineData("  latest ", KafkaFlow.AutoOffsetReset.Latest)]
    [InlineData("", KafkaFlow.AutoOffsetReset.Earliest)]
    public void ResolveAutoOffsetReset_ParsesKnownValues(string configured, KafkaFlow.AutoOffsetReset expected)
    {
        var options = new KafkaOptions { AutoOffsetReset = configured };

        Assert.Equal(expected, options.ResolveAutoOffsetReset());
    }

    [Theory]
    [InlineData("beginning")]
    [InlineData("none")]
    [InlineData("oldest")]
    public void ResolveAutoOffsetReset_Throws_OnUnknownValue(string configured)
    {
        var options = new KafkaOptions { AutoOffsetReset = configured };

        var ex = Assert.Throws<InvalidOperationException>(() => options.ResolveAutoOffsetReset());

        Assert.Contains("Kafka:AutoOffsetReset", ex.Message);
    }
}
