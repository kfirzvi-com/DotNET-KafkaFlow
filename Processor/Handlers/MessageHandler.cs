using System.Diagnostics;
using System.Text;
using KafkaFlow;
using Microsoft.Extensions.Options;
using Processor.Builders.Core;
using Processor.Diagnostics;
using Processor.Messages;
using Processor.Settings;

namespace Processor.Handlers;

public class MessageHandler : IMessageHandler<InputMessage>
{
    private readonly IMessageProducer<OutputMessage> _producer;
    private readonly IMessageProducer<DeadLetterMessage> _deadLetterProducer;
    private readonly IOutputMessageBuilder _outputMessageBuilder;
    private readonly DataTypeSettingsOptions _settingsOptions;
    private readonly ILogger<MessageHandler> _logger;

    public MessageHandler(
        IMessageProducer<OutputMessage> producer,
        IMessageProducer<DeadLetterMessage> deadLetterProducer,
        IOutputMessageBuilder outputMessageBuilder,
        IOptions<DataTypeSettingsOptions> settingsOptions,
        ILogger<MessageHandler> logger)
    {
        _producer = producer;
        _deadLetterProducer = deadLetterProducer;
        _outputMessageBuilder = outputMessageBuilder;
        _settingsOptions = settingsOptions.Value;
        _logger = logger;
    }

    public async Task Handle(IMessageContext context, InputMessage message)
    {
        var stopwatch = Stopwatch.StartNew();

        var dataTypeId = ResolveDataTypeId(context);

        _logger.LogInformation("Processing message with ID: {MessageId}, DataType: {DataTypeId}, Content: {Content}",
            message.Id, dataTypeId, message.Content);

        var outcome = await _outputMessageBuilder.Build(message, dataTypeId);

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

            case BuildStatus.Filtered:
                ProcessorMetrics.RecordFiltered(outcome.Reason ?? "filtered");
                _logger.LogInformation("Message filtered out ({Reason}): data type '{DataTypeId}'", outcome.Reason, dataTypeId);
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

    /// <summary>
    /// Reads the data type id from the configured Kafka header, falling back to the message key.
    /// </summary>
    private string? ResolveDataTypeId(IMessageContext context)
    {
        var fromHeader = context.Headers?.GetString(_settingsOptions.HeaderName);
        if (!string.IsNullOrWhiteSpace(fromHeader))
        {
            return fromHeader;
        }

        return KeyToString(context.Message.Key);
    }

    private static string? KeyToString(object? key) => key switch
    {
        null => null,
        string s => s,
        byte[] bytes => Encoding.UTF8.GetString(bytes),
        _ => key.ToString()
    };
}
