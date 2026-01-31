namespace Processor.Messages;

public class DeadLetterMessage
{
    public string Id { get; set; } = string.Empty;
    public string Reason { get; set; } = string.Empty;
    public InputMessage OriginalMessage { get; set; } = new();
    public DateTime FailedAt { get; set; }
}
