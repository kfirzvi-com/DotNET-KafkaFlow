using Processor.Builders.Core;
using Processor.Builders.FieldBuilders;
using Processor.Messages;
using Xunit;

namespace Processor.Tests.Builders;

public class ProcessedContentBuilderTests
{
    [Fact]
    public void Build_ReturnsOk_WhenContentIsPresent()
    {
        var builder = new ProcessedContentBuilder();
        var input = new InputMessage { Id = "msg-1", Content = "hello" };

        var result = builder.Build(input);

        Assert.Equal(BuildStatus.Ok, result.Status);
        Assert.Equal("HELLO", result.Value);
        Assert.Null(result.Reason);
    }

    [Fact]
    public void Build_ReturnsDrop_WhenContentIsMissing()
    {
        var builder = new ProcessedContentBuilder();
        var input = new InputMessage { Id = "msg-1", Content = "" };

        var result = builder.Build(input);

        Assert.Equal(BuildStatus.Drop, result.Status);
        Assert.Equal("Content is empty", result.Reason);
    }
}
