using Moq;
using Processor.Core.Building;
using Processor.Core.Building.FieldBuilders;
using Processor.Core.DataTypes;
using Processor.Core.Tests.Support;

namespace Processor.Core.Tests.Building;

/// <summary>
/// Core's orchestration: the data-type filter, the shared field builders, the domain builder seam, and
/// the short-circuit ordering between them.
/// </summary>
public class OutputMessageBuilderTests
{
    private const string ActiveDataType = "widgets-feed";

    private static OutputMessageBuilder<WidgetInputMessage, WidgetData> CreateBuilder(
        IDataTypeSettingsRepository repository,
        IDomainDataBuilder<WidgetInputMessage, WidgetData> domainDataBuilder) =>
        new(
            new OutputIdBuilder(),
            new ProcessedContentBuilder(),
            new ProcessedAtBuilder(),
            new ProcessorNameBuilder(),
            domainDataBuilder,
            repository);

    private static Mock<IDataTypeSettingsRepository> ActiveRepository()
    {
        var repository = new Mock<IDataTypeSettingsRepository>();
        repository
            .Setup(r => r.FindByIdAsync(ActiveDataType, It.IsAny<CancellationToken>()))
            .ReturnsAsync(new DataTypeSetting { DataTypeId = ActiveDataType, IsActive = true });
        return repository;
    }

    private static WidgetInputMessage ValidInput() => new()
    {
        Id = "msg-1",
        Content = "hello",
        DomainData = new WidgetData { Sku = "abc", Quantity = 3 }
    };

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    public async Task Build_ReturnsFiltered_WhenDataTypeIdMissing(string? dataTypeId)
    {
        var repository = new Mock<IDataTypeSettingsRepository>();
        var domainBuilder = StubWidgetDataBuilder.Passthrough();
        var builder = CreateBuilder(repository.Object, domainBuilder);

        var outcome = await builder.Build(ValidInput(), dataTypeId);

        Assert.Equal(BuildStatus.Filtered, outcome.Status);
        Assert.Equal("missing_data_type_id", outcome.Reason);
        // A missing id must not query the store, nor run domain processing.
        repository.Verify(r => r.FindByIdAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()), Times.Never);
        Assert.Equal(0, domainBuilder.Invocations);
    }

    [Fact]
    public async Task Build_ReturnsFiltered_WhenDataTypeUnknown()
    {
        var repository = new Mock<IDataTypeSettingsRepository>();
        repository
            .Setup(r => r.FindByIdAsync("nope", It.IsAny<CancellationToken>()))
            .ReturnsAsync((DataTypeSetting?)null);
        var domainBuilder = StubWidgetDataBuilder.Passthrough();

        var outcome = await CreateBuilder(repository.Object, domainBuilder).Build(ValidInput(), "nope");

        Assert.Equal(BuildStatus.Filtered, outcome.Status);
        Assert.Equal("unknown_data_type", outcome.Reason);
        Assert.Equal(0, domainBuilder.Invocations);
    }

    [Fact]
    public async Task Build_ReturnsFiltered_WhenDataTypeInactive()
    {
        var repository = new Mock<IDataTypeSettingsRepository>();
        repository
            .Setup(r => r.FindByIdAsync("retired", It.IsAny<CancellationToken>()))
            .ReturnsAsync(new DataTypeSetting { DataTypeId = "retired", IsActive = false });
        var domainBuilder = StubWidgetDataBuilder.Passthrough();

        var outcome = await CreateBuilder(repository.Object, domainBuilder).Build(ValidInput(), "retired");

        Assert.Equal(BuildStatus.Filtered, outcome.Status);
        Assert.Equal("inactive_data_type", outcome.Reason);
        Assert.Equal(0, domainBuilder.Invocations);
    }

    [Fact]
    public async Task Build_ReturnsOk_WithSharedAndDomainFieldsPopulated()
    {
        var domainBuilder = StubWidgetDataBuilder.Passthrough();
        var input = ValidInput();
        input.Timestamp = new DateTime(2026, 8, 3, 12, 0, 0, DateTimeKind.Utc);

        var outcome = await CreateBuilder(ActiveRepository().Object, domainBuilder).Build(input, ActiveDataType);

        Assert.Equal(BuildStatus.Ok, outcome.Status);
        Assert.NotNull(outcome.Message);

        var message = outcome.Message!;
        Assert.Equal("msg-1", message.Id);
        Assert.Equal("HELLO", message.ProcessedContent);
        Assert.Equal(input.Timestamp, message.ProcessedAt);
        Assert.Equal(Environment.MachineName, message.ProcessorName);
        // Domain name comes off the payload, so it cannot drift from the data being produced.
        Assert.Equal(WidgetData.Domain, message.Domain);
        Assert.Equal("ABC", message.DomainData.Sku);
        Assert.Equal(3, message.DomainData.Quantity);
        Assert.Equal(1, domainBuilder.Invocations);
    }

    [Fact]
    public async Task Build_ReturnsDeadLetter_WhenSharedFieldFails_WithoutRunningDomainBuilder()
    {
        var domainBuilder = StubWidgetDataBuilder.Passthrough();
        var input = ValidInput();
        input.Id = "  ";

        var outcome = await CreateBuilder(ActiveRepository().Object, domainBuilder).Build(input, ActiveDataType);

        Assert.Equal(BuildStatus.DeadLetter, outcome.Status);
        Assert.Equal("Missing message id", outcome.Reason);
        // Domain processing is the last step: shared rejections must not pay for it.
        Assert.Equal(0, domainBuilder.Invocations);
    }

    [Fact]
    public async Task Build_ReturnsDrop_WhenContentEmpty_WithoutRunningDomainBuilder()
    {
        var domainBuilder = StubWidgetDataBuilder.Passthrough();
        var input = ValidInput();
        input.Content = "";

        var outcome = await CreateBuilder(ActiveRepository().Object, domainBuilder).Build(input, ActiveDataType);

        Assert.Equal(BuildStatus.Drop, outcome.Status);
        Assert.Equal("Content is empty", outcome.Reason);
        Assert.Equal(0, domainBuilder.Invocations);
    }

    [Fact]
    public async Task Build_ReturnsDeadLetter_WhenDomainBuilderDeadLetters()
    {
        var domainBuilder = StubWidgetDataBuilder.DeadLettering("Bad widget payload");

        var outcome = await CreateBuilder(ActiveRepository().Object, domainBuilder)
            .Build(ValidInput(), ActiveDataType);

        Assert.Equal(BuildStatus.DeadLetter, outcome.Status);
        Assert.Equal("Bad widget payload", outcome.Reason);
        Assert.Null(outcome.Message);
    }

    [Fact]
    public async Task Build_ReturnsDrop_WhenDomainBuilderDrops()
    {
        var domainBuilder = StubWidgetDataBuilder.Dropping("Uninteresting widget");

        var outcome = await CreateBuilder(ActiveRepository().Object, domainBuilder)
            .Build(ValidInput(), ActiveDataType);

        Assert.Equal(BuildStatus.Drop, outcome.Status);
        Assert.Equal("Uninteresting widget", outcome.Reason);
        Assert.Null(outcome.Message);
    }

    [Fact]
    public async Task Build_SubstitutesEmptyPayload_WhenDomainBuilderReturnsOkWithNull()
    {
        var domainBuilder = StubWidgetDataBuilder.OkWithNullValue();

        var outcome = await CreateBuilder(ActiveRepository().Object, domainBuilder)
            .Build(ValidInput(), ActiveDataType);

        Assert.Equal(BuildStatus.Ok, outcome.Status);
        // The output message must never carry a null payload downstream.
        Assert.NotNull(outcome.Message!.DomainData);
        Assert.Equal(WidgetData.Domain, outcome.Message!.Domain);
    }

    [Fact]
    public async Task Build_PassesCancellationTokenToRepository()
    {
        using var cts = new CancellationTokenSource();
        var repository = new Mock<IDataTypeSettingsRepository>();
        repository
            .Setup(r => r.FindByIdAsync(ActiveDataType, cts.Token))
            .ReturnsAsync(new DataTypeSetting { DataTypeId = ActiveDataType, IsActive = true });

        var outcome = await CreateBuilder(repository.Object, StubWidgetDataBuilder.Passthrough())
            .Build(ValidInput(), ActiveDataType, cts.Token);

        Assert.Equal(BuildStatus.Ok, outcome.Status);
        repository.Verify(r => r.FindByIdAsync(ActiveDataType, cts.Token), Times.Once);
    }
}
