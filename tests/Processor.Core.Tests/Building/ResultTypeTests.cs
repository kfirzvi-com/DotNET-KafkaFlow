using Processor.Core.Building;
using Processor.Core.Messages;
using Processor.Core.Tests.Support;

namespace Processor.Core.Tests.Building;

/// <summary>
/// The result wrappers carry the routing decision through the pipeline, so their invariants (a failed
/// result never carries a value; a successful one never carries a reason) are worth pinning.
/// </summary>
public class ResultTypeTests
{
    [Fact]
    public void FieldBuildResult_Ok_CarriesValueAndNoReason()
    {
        var result = FieldBuildResult<string>.Ok("value");

        Assert.Equal(BuildStatus.Ok, result.Status);
        Assert.Equal("value", result.Value);
        Assert.Null(result.Reason);
    }

    [Fact]
    public void FieldBuildResult_DeadLetter_CarriesReasonAndNoValue()
    {
        var result = FieldBuildResult<string>.DeadLetter("boom");

        Assert.Equal(BuildStatus.DeadLetter, result.Status);
        Assert.Null(result.Value);
        Assert.Equal("boom", result.Reason);
    }

    [Fact]
    public void FieldBuildResult_Drop_CarriesReasonAndNoValue()
    {
        var result = FieldBuildResult<int>.Drop("nope");

        Assert.Equal(BuildStatus.Drop, result.Status);
        Assert.Equal(0, result.Value);
        Assert.Equal("nope", result.Reason);
    }

    [Fact]
    public void BuildOutcome_Ok_CarriesMessage()
    {
        var message = new OutputMessage<WidgetData> { Id = "m1" };

        var outcome = BuildOutcome<WidgetData>.Ok(message);

        Assert.Equal(BuildStatus.Ok, outcome.Status);
        Assert.Same(message, outcome.Message);
        Assert.Null(outcome.Reason);
    }

    [Theory]
    [InlineData(BuildStatus.DeadLetter)]
    [InlineData(BuildStatus.Drop)]
    [InlineData(BuildStatus.Filtered)]
    public void BuildOutcome_Failures_CarryReasonAndNoMessage(BuildStatus status)
    {
        var outcome = status switch
        {
            BuildStatus.DeadLetter => BuildOutcome<WidgetData>.DeadLetter("reason"),
            BuildStatus.Drop => BuildOutcome<WidgetData>.Drop("reason"),
            _ => BuildOutcome<WidgetData>.Filtered("reason")
        };

        Assert.Equal(status, outcome.Status);
        Assert.Null(outcome.Message);
        Assert.Equal("reason", outcome.Reason);
    }

    [Fact]
    public void DeadLetterOriginal_From_CopiesSharedFieldsAndPayload()
    {
        var payload = new WidgetData { Sku = "sku-1", Quantity = 7 };
        var input = new WidgetInputMessage
        {
            Id = "m1",
            Content = "content",
            Timestamp = new DateTime(2026, 8, 3, 0, 0, 0, DateTimeKind.Utc),
            DomainData = payload
        };

        var original = DeadLetterOriginal<WidgetData>.From(input);

        Assert.Equal("m1", original.Id);
        Assert.Equal("content", original.Content);
        Assert.Equal(input.Timestamp, original.Timestamp);
        Assert.Same(payload, original.DomainData);
    }

    [Fact]
    public void InputMessage_DomainData_DefaultsToEmptyPayload_NotNull()
    {
        var input = new WidgetInputMessage();

        Assert.NotNull(input.DomainData);
        Assert.Equal(WidgetData.Domain, input.DomainData.DomainName);
    }
}
