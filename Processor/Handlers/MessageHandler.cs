using KafkaFlow;
using Processor.Messages;

namespace Processor.Handlers;

public class MessageHandler : IMessageHandler<InputMessage>
{
    private readonly IMessageProducer<OutputMessage> _producer;
    private readonly ILogger<MessageHandler> _logger;

    public MessageHandler(IMessageProducer<OutputMessage> producer, ILogger<MessageHandler> logger)
    {
        _producer = producer;
        _logger = logger;
    }

    public async Task Handle(IMessageContext context, InputMessage message)
    {
        _logger.LogInformation("Processing message with ID: {MessageId}, Content: {Content}", 
            message.Id, message.Content);

        // Process the input message
        var processedContent = ProcessMessage(message.Content);

        // Create output message
        var outputMessage = new OutputMessage
        {
            Id = message.Id,
            ProcessedContent = processedContent,
            ProcessedAt = DateTime.UtcNow,
            ProcessorName = Environment.MachineName
        };

        // Send to output topic
        await _producer.ProduceAsync(message.Id, outputMessage);

        _logger.LogInformation("Message processed and sent to output topic");
    }

    private string ProcessMessage(string content)
    {
        // Simple processing: convert to uppercase
        return content.ToUpperInvariant();
    }
}
