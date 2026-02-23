using System.Diagnostics;
using KafkaFlow;
using Processor.Builders.Core;
using Processor.Diagnostics;
using Processor.Messages;

namespace Processor.Handlers;

public class MessageHandler : IMessageHandler<InputMessage>
{
    private readonly IMessageProducer<OutputMessage> _producer;
    private readonly IMessageProducer<DeadLetterMessage> _deadLetterProducer;
    private readonly IOutputMessageBuilder _outputMessageBuilder;
    private readonly ILogger<MessageHandler> _logger;

    public MessageHandler(
        IMessageProducer<OutputMessage> producer,
        IMessageProducer<DeadLetterMessage> deadLetterProducer,
        IOutputMessageBuilder outputMessageBuilder,
        ILogger<MessageHandler> logger)
    {
        _producer = producer;
        _deadLetterProducer = deadLetterProducer;
        _outputMessageBuilder = outputMessageBuilder;
        _logger = logger;
    }

    public async Task Handle(IMessageContext context, InputMessage message)
    {
        var stopwatch = Stopwatch.StartNew();

        _logger.LogInformation("Processing message with ID: {MessageId}, Content: {Content}",
            message.Id, message.Content);

        var outcome = _outputMessageBuilder.Build(message);

        switch (outcome.Status)
        {
            case BuildStatus.Ok:
                await _producer.ProduceAsync(message.Id, outcome.Message!);
                ProcessorMetrics.RecordProcessed();
                _logger.LogInformation("Message processed and sent to output topic");
                break;

            case BuildStatus.DeadLetter:
                var deadLetterMessage = new DeadLetterMessage
                {
                    Id = message.Id,
                    Reason = outcome.Reason ?? "Unknown reason",
                    OriginalMessage = message,
                    FailedAt = DateTime.UtcNow
                };

                await _deadLetterProducer.ProduceAsync(message.Id, deadLetterMessage);
                ProcessorMetrics.RecordDeadLettered();
                _logger.LogWarning("Message sent to dead letter queue: {Reason}", outcome.Reason);
                break;

            case BuildStatus.Drop:
                ProcessorMetrics.RecordDropped();
                _logger.LogWarning("Message dropped: {Reason}", outcome.Reason);
                break;

            default:
                _logger.LogWarning("Unhandled build status: {Status}", outcome.Status);
                break;
        }

        stopwatch.Stop();
        ProcessorMetrics.RecordProcessingDuration(stopwatch.Elapsed.TotalMilliseconds);
    }
}
