using KafkaFlow;

namespace Processor.Diagnostics;

public class MicrosoftLogHandler : ILogHandler
{
    private readonly ILogger<MicrosoftLogHandler> _logger;

    public MicrosoftLogHandler(ILogger<MicrosoftLogHandler> logger)
    {
        _logger = logger;
    }

    public void Error(string message, Exception ex, object data) =>
        _logger.LogError(ex, "[KafkaFlow] {Message} | {@Data}", message, data);

    public void Warning(string message, object data) =>
        _logger.LogWarning("[KafkaFlow] {Message} | {@Data}", message, data);

    public void Warning(string message, Exception ex, object data) =>
        _logger.LogWarning(ex, "[KafkaFlow] {Message} | {@Data}", message, data);

    public void Info(string message, object data) =>
        _logger.LogInformation("[KafkaFlow] {Message} | {@Data}", message, data);

    public void Verbose(string message, object data) =>
        _logger.LogDebug("[KafkaFlow] {Message} | {@Data}", message, data);
}
