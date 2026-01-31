using Processor.Builders.Core;
using Processor.Builders.FieldBuilders;
using Processor.Messages;
using Xunit;

namespace Processor.Tests.Builders;

public class OutputIdBuilderTests
{
    [Fact]
    public void Build_ReturnsOk_WhenIdIsPresent()
    {
        var builder = new OutputIdBuilder();
        var input = new InputMessage { Id = "msg-1", Content = "hello" };

        var result = builder.Build(input);

        Assert.Equal(BuildStatus.Ok, result.Status);
        Assert.Equal("msg-1", result.Value);
        Assert.Null(result.Reason);
    }

    [Fact]
    public void Build_ReturnsDeadLetter_WhenIdIsMissing()
    {
        var builder = new OutputIdBuilder();
        var input = new InputMessage { Id = " ", Content = "hello" };

        var result = builder.Build(input);

        Assert.Equal(BuildStatus.DeadLetter, result.Status);
        Assert.Equal("Missing message id", result.Reason);
    }
}
