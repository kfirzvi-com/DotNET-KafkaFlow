using Moq;
using Processor.Builders.Core;
using Processor.Builders.FieldBuilders;
using Processor.Messages;
using Processor.Settings;
using Xunit;

namespace Processor.Tests.Builders;

public class OutputMessageBuilderFilterTests
{
    private static OutputMessageBuilder CreateBuilder(IDataTypeSettingsRepository repository) =>
        new(
            new OutputIdBuilder(),
            new ProcessedContentBuilder(),
            new ProcessedAtBuilder(),
            new ProcessorNameBuilder(),
            repository);

    private static InputMessage ValidInput() => new() { Id = "msg-1", Content = "hello" };

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    public async Task Build_ReturnsFiltered_WhenDataTypeIdMissing(string? dataTypeId)
    {
        var repo = new Mock<IDataTypeSettingsRepository>();
        var builder = CreateBuilder(repo.Object);

        var outcome = await builder.Build(ValidInput(), dataTypeId);

        Assert.Equal(BuildStatus.Filtered, outcome.Status);
        // A missing id must not query the repository.
        repo.Verify(r => r.FindByIdAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()), Times.Never);
    }

    [Fact]
    public async Task Build_ReturnsFiltered_WhenDataTypeUnknown()
    {
        var repo = new Mock<IDataTypeSettingsRepository>();
        repo.Setup(r => r.FindByIdAsync("weather", It.IsAny<CancellationToken>()))
            .ReturnsAsync((DataTypeSetting?)null);
        var builder = CreateBuilder(repo.Object);

        var outcome = await builder.Build(ValidInput(), "weather");

        Assert.Equal(BuildStatus.Filtered, outcome.Status);
    }

    [Fact]
    public async Task Build_ReturnsFiltered_WhenDataTypeInactive()
    {
        var repo = new Mock<IDataTypeSettingsRepository>();
        repo.Setup(r => r.FindByIdAsync("weather", It.IsAny<CancellationToken>()))
            .ReturnsAsync(new DataTypeSetting { DataTypeId = "weather", IsActive = false });
        var builder = CreateBuilder(repo.Object);

        var outcome = await builder.Build(ValidInput(), "weather");

        Assert.Equal(BuildStatus.Filtered, outcome.Status);
    }

    [Fact]
    public async Task Build_ReturnsOk_WhenDataTypeActiveAndMessageValid()
    {
        var repo = new Mock<IDataTypeSettingsRepository>();
        repo.Setup(r => r.FindByIdAsync("weather", It.IsAny<CancellationToken>()))
            .ReturnsAsync(new DataTypeSetting { DataTypeId = "weather", IsActive = true });
        var builder = CreateBuilder(repo.Object);

        var outcome = await builder.Build(ValidInput(), "weather");

        Assert.Equal(BuildStatus.Ok, outcome.Status);
        Assert.NotNull(outcome.Message);
        Assert.Equal("HELLO", outcome.Message!.ProcessedContent);
    }
}
