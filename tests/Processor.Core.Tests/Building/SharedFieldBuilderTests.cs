using Processor.Core.Building;
using Processor.Core.Building.FieldBuilders;
using Processor.Core.Tests.Support;

namespace Processor.Core.Tests.Building;

/// <summary>
/// The shared field builders, exercised through a synthetic domain's message type. That they compile
/// and run against <c>WidgetInputMessage</c> at all is part of what is under test: contravariance is
/// what lets one registration serve every domain.
/// </summary>
public class SharedFieldBuilderTests
{
    private static WidgetInputMessage Input(
        string id = "msg-1", string content = "hello", DateTime timestamp = default) => new()
        {
            Id = id,
            Content = content,
            Timestamp = timestamp,
            DomainData = new WidgetData { Sku = "abc", Quantity = 1 }
        };

    [Fact]
    public void OutputId_ReturnsOk_AndTrims_WhenIdIsPresent()
    {
        var result = new OutputIdBuilder().Build(Input(id: "  msg-1  "));

        Assert.Equal(BuildStatus.Ok, result.Status);
        Assert.Equal("msg-1", result.Value);
        Assert.Null(result.Reason);
    }

    [Theory]
    [InlineData("")]
    [InlineData(" ")]
    [InlineData("\t")]
    public void OutputId_ReturnsDeadLetter_WhenIdIsMissing(string id)
    {
        var result = new OutputIdBuilder().Build(Input(id: id));

        Assert.Equal(BuildStatus.DeadLetter, result.Status);
        Assert.Equal("Missing message id", result.Reason);
    }

    [Fact]
    public void ProcessedContent_ReturnsOk_Uppercased_WhenContentIsPresent()
    {
        var result = new ProcessedContentBuilder().Build(Input(content: "hello world"));

        Assert.Equal(BuildStatus.Ok, result.Status);
        Assert.Equal("HELLO WORLD", result.Value);
        Assert.Null(result.Reason);
    }

    [Theory]
    [InlineData("")]
    [InlineData("   ")]
    public void ProcessedContent_ReturnsDrop_WhenContentIsMissing(string content)
    {
        var result = new ProcessedContentBuilder().Build(Input(content: content));

        Assert.Equal(BuildStatus.Drop, result.Status);
        Assert.Equal("Content is empty", result.Reason);
    }

    [Fact]
    public void ProcessedAt_PreservesTimestamp_WhenProvided()
    {
        var timestamp = new DateTime(2026, 8, 3, 10, 30, 0, DateTimeKind.Utc);

        var result = new ProcessedAtBuilder().Build(Input(timestamp: timestamp));

        Assert.Equal(BuildStatus.Ok, result.Status);
        Assert.Equal(timestamp, result.Value);
    }

    [Fact]
    public void ProcessedAt_SubstitutesNow_WhenTimestampIsDefault()
    {
        var before = DateTime.UtcNow.AddSeconds(-1);

        var result = new ProcessedAtBuilder().Build(Input(timestamp: default));

        Assert.Equal(BuildStatus.Ok, result.Status);
        Assert.InRange(result.Value, before, DateTime.UtcNow.AddSeconds(1));
    }

    [Fact]
    public void ProcessorName_ReturnsMachineName()
    {
        var result = new ProcessorNameBuilder().Build(Input());

        Assert.Equal(BuildStatus.Ok, result.Status);
        Assert.Equal(Environment.MachineName, result.Value);
    }
}
