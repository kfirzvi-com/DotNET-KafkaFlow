namespace Processor.Messages;

public class InputMessage
{
    public string Id { get; set; } = string.Empty;
    public string Content { get; set; } = string.Empty;
    public DateTime Timestamp { get; set; }
}
