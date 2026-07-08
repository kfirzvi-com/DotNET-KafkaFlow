using Xunit;
using Moq;
using KafkaFlow;
using Processor.Builders.Core;
using Processor.Builders.FieldBuilders;
using Processor.Handlers;
using Processor.Messages;
using Processor.Settings;
using Processor.Tests.Helpers;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Confluent.Kafka;

namespace Processor.Tests;

public class MessageHandlerTests
{
    [Theory]
    [MemberData(nameof(TestDataLoader.TestCases), MemberType = typeof(TestDataLoader))]
    public async Task Handle_ProcessesMessageCorrectly(string fileName, string fileContent)
    {
        var data = DeserializeTestData(fileName, fileContent);

        // Arrange
        const string dataTypeId = "test-type";

        var mockProducer = new Mock<IMessageProducer<OutputMessage>>();
        var mockDeadLetterProducer = new Mock<IMessageProducer<DeadLetterMessage>>();
        var mockLogger = new Mock<ILogger<MessageHandler>>();

        // Data type is active, so records pass the filter and exercise the existing pipeline.
        var mockRepository = new Mock<IDataTypeSettingsRepository>();
        mockRepository
            .Setup(r => r.FindByIdAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new DataTypeSetting { DataTypeId = dataTypeId, IsActive = true });

        // The handler reads the data type id from the message key (no header set up here).
        var mockContext = new Mock<IMessageContext>();
        mockContext.Setup(c => c.Message).Returns(new Message(dataTypeId, data.Input!));

        var options = Options.Create(new DataTypeSettingsOptions());

        var outputMessageBuilder = new OutputMessageBuilder(
            new OutputIdBuilder(),
            new ProcessedContentBuilder(),
            new ProcessedAtBuilder(),
            new ProcessorNameBuilder(),
            mockRepository.Object);

        var handler = new MessageHandler(
            mockProducer.Object,
            mockDeadLetterProducer.Object,
            outputMessageBuilder,
            options,
            Options.Create(new BenchmarkOptions()),
            mockLogger.Object);

        // Setup the mock to return a DeliveryResult
        var deliveryResult = new DeliveryResult<byte[], byte[]>
        {
            Status = PersistenceStatus.Persisted,
            Offset = new Offset(0),
            Partition = new Partition(0),
            Topic = "output-topic"
        };

        mockProducer
            .Setup(p => p.ProduceAsync(It.IsAny<object>(), It.IsAny<OutputMessage>(), It.IsAny<KafkaFlow.IMessageHeaders>(), It.IsAny<int?>()))
            .Returns(Task.FromResult(deliveryResult));

        mockDeadLetterProducer
            .Setup(p => p.ProduceAsync(It.IsAny<object>(), It.IsAny<DeadLetterMessage>(), It.IsAny<KafkaFlow.IMessageHeaders>(), It.IsAny<int?>()))
            .Returns(Task.FromResult(deliveryResult));

        // Act
        await handler.Handle(mockContext.Object, data.Input!);

        // Assert - Verify based on expected outcome
        switch (data.ExpectedOutcome.ToLowerInvariant())
        {
            case "output":
                VerifyMessageSentToOutput(mockProducer, data);
                VerifyMessageNotSentToDeadLetter(mockDeadLetterProducer);
                break;

            case "deadletter":
                VerifyMessageNotSentToOutput(mockProducer);
                VerifyMessageSentToDeadLetter(mockDeadLetterProducer, data);
                break;

            case "dropped":
                VerifyMessageNotSentToOutput(mockProducer);
                VerifyMessageNotSentToDeadLetter(mockDeadLetterProducer);
                var dropOutcome = await outputMessageBuilder.Build(data.Input!, dataTypeId);
                Assert.Equal(BuildStatus.Drop, dropOutcome.Status);
                Assert.Equal(data.ExpectedDropReason, dropOutcome.Reason);
                break;

            default:
                throw new InvalidOperationException($"Unknown expected outcome: {data.ExpectedOutcome}");
        }
    }

    private static void VerifyMessageSentToOutput(Mock<IMessageProducer<OutputMessage>> mockProducer, Helpers.TestData data)
    {
        mockProducer.Verify(
            p => p.ProduceAsync(
                It.Is<object>(key => key.ToString() == data.Input!.Id),
                It.Is<OutputMessage>(msg =>
                    msg.Id == data.ExpectedOutput!.Id &&
                    msg.ProcessedContent == data.ExpectedOutput!.ProcessedContent),
                It.IsAny<KafkaFlow.IMessageHeaders>(),
                It.IsAny<int?>()),
            Times.Once,
            "Message should be sent to output queue"
        );
    }

    private static void VerifyMessageNotSentToOutput(Mock<IMessageProducer<OutputMessage>> mockProducer)
    {
        mockProducer.Verify(
            p => p.ProduceAsync(It.IsAny<object>(), It.IsAny<OutputMessage>(), It.IsAny<KafkaFlow.IMessageHeaders>(), It.IsAny<int?>()),
            Times.Never,
            "Message should not be sent to output queue"
        );
    }

    private static void VerifyMessageSentToDeadLetter(Mock<IMessageProducer<DeadLetterMessage>> mockProducer, Helpers.TestData data)
    {
        mockProducer.Verify(
            p => p.ProduceAsync(
                It.Is<object>(key => key.ToString() == data.Input!.Id),
                It.Is<DeadLetterMessage>(msg =>
                    msg.Reason == data.ExpectedDeadLetterReason &&
                    msg.OriginalMessage.Id == data.Input!.Id &&
                    msg.OriginalMessage.Content == data.Input!.Content),
                It.IsAny<KafkaFlow.IMessageHeaders>(),
                It.IsAny<int?>()),
            Times.Once,
            "Message should be sent to dead letter queue"
        );
    }

    private static void VerifyMessageNotSentToDeadLetter(Mock<IMessageProducer<DeadLetterMessage>> mockProducer)
    {
        mockProducer.Verify(
            p => p.ProduceAsync(It.IsAny<object>(), It.IsAny<DeadLetterMessage>(), It.IsAny<KafkaFlow.IMessageHeaders>(), It.IsAny<int?>()),
            Times.Never,
            "Message should not be sent to dead letter queue"
        );
    }

    private static Helpers.TestData DeserializeTestData(string fileName, string fileContent)
    {
        var options = new System.Text.Json.JsonSerializerOptions
        {
            PropertyNameCaseInsensitive = true
        };

        try
        {
            var data = System.Text.Json.JsonSerializer.Deserialize<Helpers.TestData>(fileContent, options);

            if (data?.Input == null)
            {
                throw new InvalidOperationException("Invalid test data format: missing input");
            }

            var expectedOutcome = data.ExpectedOutcome.ToLowerInvariant();

            if (expectedOutcome == "output" && data.ExpectedOutput == null)
            {
                throw new InvalidOperationException("Invalid test data format: expectedOutput is required for 'output' outcome");
            }

            if (expectedOutcome == "deadletter" && string.IsNullOrWhiteSpace(data.ExpectedDeadLetterReason))
            {
                throw new InvalidOperationException("Invalid test data format: expectedDeadLetterReason is required for 'deadletter' outcome");
            }

            if (expectedOutcome == "dropped" && string.IsNullOrWhiteSpace(data.ExpectedDropReason))
            {
                throw new InvalidOperationException("Invalid test data format: expectedDropReason is required for 'dropped' outcome");
            }

            return data;
        }
        catch (System.Text.Json.JsonException ex)
        {
            throw new InvalidOperationException($"{fileName}: Error parsing JSON: {ex.Message}", ex);
        }
        catch (InvalidOperationException)
        {
            throw;
        }
        catch (Exception ex)
        {
            throw new InvalidOperationException($"{fileName}: Unexpected error: {ex.Message}", ex);
        }
    }
}
