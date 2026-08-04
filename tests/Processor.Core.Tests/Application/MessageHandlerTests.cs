using Confluent.Kafka;
using KafkaFlow;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Moq;
using Processor.Core.Application;
using Processor.Core.Building;
using Processor.Core.Messages;
using Processor.Core.Tests.Support;
using IMessageHeaders = KafkaFlow.IMessageHeaders;

namespace Processor.Core.Tests.Application;

/// <summary>
/// The shared handler's routing: which producer each build status reaches, how the data type id is
/// resolved from the Kafka header or key, and that the domain stamped on outgoing messages comes from
/// the payload type.
/// </summary>
public class MessageHandlerTests
{
    private const string HeaderName = "data-type-id";

    /// <summary>Builder whose outcome the test dictates, capturing what it was asked to build.</summary>
    private sealed class StubOutputMessageBuilder : IOutputMessageBuilder<WidgetInputMessage, WidgetData>
    {
        private readonly BuildOutcome<WidgetData> _outcome;

        public StubOutputMessageBuilder(BuildOutcome<WidgetData> outcome) => _outcome = outcome;

        public string? LastDataTypeId { get; private set; }

        public int Calls { get; private set; }

        public Task<BuildOutcome<WidgetData>> Build(
            WidgetInputMessage input, string? dataTypeId, CancellationToken cancellationToken = default)
        {
            Calls++;
            LastDataTypeId = dataTypeId;
            return Task.FromResult(_outcome);
        }
    }

    private sealed class Harness
    {
        public Harness(BuildOutcome<WidgetData> outcome)
        {
            Builder = new StubOutputMessageBuilder(outcome);

            var deliveryResult = new DeliveryResult<byte[], byte[]>
            {
                Status = PersistenceStatus.Persisted,
                Offset = new Offset(0),
                Partition = new Partition(0),
                Topic = "output"
            };

            OutputProducer
                .Setup(p => p.ProduceAsync(
                    It.IsAny<object>(), It.IsAny<OutputMessage<WidgetData>>(),
                    It.IsAny<IMessageHeaders>(), It.IsAny<int?>()))
                .ReturnsAsync(deliveryResult);

            DeadLetterProducer
                .Setup(p => p.ProduceAsync(
                    It.IsAny<object>(), It.IsAny<DeadLetterMessage<WidgetData>>(),
                    It.IsAny<IMessageHeaders>(), It.IsAny<int?>()))
                .ReturnsAsync(deliveryResult);

            Handler = new MessageHandler<WidgetInputMessage, WidgetData>(
                OutputProducer.Object,
                DeadLetterProducer.Object,
                Builder,
                Options.Create(new DataTypeSettingsOptions { HeaderName = HeaderName }),
                Options.Create(new BenchmarkOptions()),
                NullLogger<MessageHandler<WidgetInputMessage, WidgetData>>.Instance);
        }

        public Mock<IMessageProducer<OutputMessage<WidgetData>>> OutputProducer { get; } = new();

        public Mock<IMessageProducer<DeadLetterMessage<WidgetData>>> DeadLetterProducer { get; } = new();

        public StubOutputMessageBuilder Builder { get; }

        public MessageHandler<WidgetInputMessage, WidgetData> Handler { get; }

        public void VerifyNoOutput() => OutputProducer.Verify(
            p => p.ProduceAsync(
                It.IsAny<object>(), It.IsAny<OutputMessage<WidgetData>>(),
                It.IsAny<IMessageHeaders>(), It.IsAny<int?>()),
            Times.Never);

        public void VerifyNoDeadLetter() => DeadLetterProducer.Verify(
            p => p.ProduceAsync(
                It.IsAny<object>(), It.IsAny<DeadLetterMessage<WidgetData>>(),
                It.IsAny<IMessageHeaders>(), It.IsAny<int?>()),
            Times.Never);
    }

    private static WidgetInputMessage Input() => new()
    {
        Id = "msg-1",
        Content = "hello",
        DomainData = new WidgetData { Sku = "abc", Quantity = 2 }
    };

    private static OutputMessage<WidgetData> OkMessage() => new()
    {
        Id = "msg-1",
        ProcessedContent = "HELLO",
        Domain = WidgetData.Domain,
        DomainData = new WidgetData { Sku = "ABC", Quantity = 2 }
    };

    /// <summary>
    /// Message context with an optional key and optional header. Uses KafkaFlow's real
    /// <see cref="MessageHeaders"/> because <c>GetString</c> is an extension method and cannot be
    /// mocked — which also means this exercises the actual header decoding.
    /// </summary>
    private static IMessageContext ContextWith(object? key, string? headerValue = null)
    {
        var headers = new MessageHeaders();
        if (headerValue is not null)
        {
            headers.Add(HeaderName, System.Text.Encoding.UTF8.GetBytes(headerValue));
        }

        var context = new Mock<IMessageContext>();
        context.Setup(c => c.Message).Returns(new Message(key!, new object()));
        context.Setup(c => c.Headers).Returns(headers);
        return context.Object;
    }

    [Fact]
    public async Task Handle_ProducesToOutput_OnOk()
    {
        var harness = new Harness(BuildOutcome<WidgetData>.Ok(OkMessage()));

        await harness.Handler.Handle(ContextWith("widgets-feed"), Input());

        harness.OutputProducer.Verify(
            p => p.ProduceAsync(
                It.Is<object>(key => key.ToString() == "msg-1"),
                It.Is<OutputMessage<WidgetData>>(m => m.Id == "msg-1" && m.ProcessedContent == "HELLO"),
                It.IsAny<IMessageHeaders>(), It.IsAny<int?>()),
            Times.Once);
        harness.VerifyNoDeadLetter();
    }

    [Fact]
    public async Task Handle_ProducesToDeadLetter_OnDeadLetter_WithOriginalPreserved()
    {
        var harness = new Harness(BuildOutcome<WidgetData>.DeadLetter("Missing message id"));
        var input = Input();

        await harness.Handler.Handle(ContextWith("widgets-feed"), input);

        harness.DeadLetterProducer.Verify(
            p => p.ProduceAsync(
                It.Is<object>(key => key.ToString() == "msg-1"),
                It.Is<DeadLetterMessage<WidgetData>>(m =>
                    m.Reason == "Missing message id" &&
                    m.Domain == WidgetData.Domain &&
                    m.OriginalMessage.Id == input.Id &&
                    m.OriginalMessage.Content == input.Content &&
                    // The domain payload must survive into the envelope, not be flattened away.
                    m.OriginalMessage.DomainData.Sku == "abc"),
                It.IsAny<IMessageHeaders>(), It.IsAny<int?>()),
            Times.Once);
        harness.VerifyNoOutput();
    }

    [Fact]
    public async Task Handle_ProducesNothing_OnDrop()
    {
        var harness = new Harness(BuildOutcome<WidgetData>.Drop("Content is empty"));

        await harness.Handler.Handle(ContextWith("widgets-feed"), Input());

        harness.VerifyNoOutput();
        harness.VerifyNoDeadLetter();
    }

    [Fact]
    public async Task Handle_ProducesNothing_OnFiltered()
    {
        var harness = new Harness(BuildOutcome<WidgetData>.Filtered("inactive_data_type"));

        await harness.Handler.Handle(ContextWith("retired"), Input());

        harness.VerifyNoOutput();
        harness.VerifyNoDeadLetter();
    }

    [Fact]
    public async Task Handle_PrefersHeaderOverKey_ForDataTypeId()
    {
        var harness = new Harness(BuildOutcome<WidgetData>.Ok(OkMessage()));

        await harness.Handler.Handle(ContextWith("from-key", headerValue: "from-header"), Input());

        Assert.Equal("from-header", harness.Builder.LastDataTypeId);
    }

    [Fact]
    public async Task Handle_FallsBackToStringKey_WhenHeaderAbsent()
    {
        var harness = new Harness(BuildOutcome<WidgetData>.Ok(OkMessage()));

        await harness.Handler.Handle(ContextWith("from-key"), Input());

        Assert.Equal("from-key", harness.Builder.LastDataTypeId);
    }

    [Fact]
    public async Task Handle_DecodesByteArrayKey_AsUtf8()
    {
        var harness = new Harness(BuildOutcome<WidgetData>.Ok(OkMessage()));
        var key = System.Text.Encoding.UTF8.GetBytes("binary-key");

        await harness.Handler.Handle(ContextWith(key), Input());

        Assert.Equal("binary-key", harness.Builder.LastDataTypeId);
    }

    [Fact]
    public async Task Handle_PassesNullDataTypeId_WhenNeitherHeaderNorKeyPresent()
    {
        var harness = new Harness(BuildOutcome<WidgetData>.Filtered("missing_data_type_id"));

        await harness.Handler.Handle(ContextWith(key: null), Input());

        Assert.Null(harness.Builder.LastDataTypeId);
        Assert.Equal(1, harness.Builder.Calls);
    }

    [Theory]
    [InlineData("")]
    [InlineData("   ")]
    public async Task Handle_IgnoresBlankHeader_AndFallsBackToKey(string headerValue)
    {
        var harness = new Harness(BuildOutcome<WidgetData>.Ok(OkMessage()));

        await harness.Handler.Handle(ContextWith("from-key", headerValue), Input());

        Assert.Equal("from-key", harness.Builder.LastDataTypeId);
    }
}
