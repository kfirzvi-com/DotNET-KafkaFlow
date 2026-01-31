using Xunit;
using Moq;
using KafkaFlow;
using Processor.Handlers;
using Processor.Messages;
using Processor.Tests.Helpers;
using Microsoft.Extensions.Logging;
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
        var mockProducer = new Mock<IMessageProducer<OutputMessage>>();
        var mockDeadLetterProducer = new Mock<IMessageProducer<OutputMessage>>();
        var mockLogger = new Mock<ILogger<MessageHandler>>();
        var mockContext = new Mock<IMessageContext>();

        var handler = new MessageHandler(mockProducer.Object, mockLogger.Object);

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
            .Setup(p => p.ProduceAsync(It.IsAny<object>(), It.IsAny<OutputMessage>(), It.IsAny<KafkaFlow.IMessageHeaders>(), It.IsAny<int?>()))
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

    private static void VerifyMessageSentToDeadLetter(Mock<IMessageProducer<OutputMessage>> mockProducer, Helpers.TestData data)
    {
        mockProducer.Verify(
            p => p.ProduceAsync(
                It.Is<object>(key => key.ToString() == data.Input!.Id),
                It.IsAny<OutputMessage>(),
                It.IsAny<KafkaFlow.IMessageHeaders>(),
                It.IsAny<int?>()),
            Times.Once,
            "Message should be sent to dead letter queue"
        );
    }

    private static void VerifyMessageNotSentToDeadLetter(Mock<IMessageProducer<OutputMessage>> mockProducer)
    {
        mockProducer.Verify(
            p => p.ProduceAsync(It.IsAny<object>(), It.IsAny<OutputMessage>(), It.IsAny<KafkaFlow.IMessageHeaders>(), It.IsAny<int?>()),
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

            // expectedOutput is only required for 'output' outcome
            if (data.ExpectedOutcome.Equals("output", StringComparison.OrdinalIgnoreCase) && data.ExpectedOutput == null)
            {
                throw new InvalidOperationException("Invalid test data format: expectedOutput is required for 'output' outcome");
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
