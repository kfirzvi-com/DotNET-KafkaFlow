namespace Processor.Domain.Messages;

public class OutputMessage
{
    public string Id { get; set; } = string.Empty;
    public string ProcessedContent { get; set; } = string.Empty;
    public DateTime ProcessedAt { get; set; }
    public string ProcessorName { get; set; } = string.Empty;
}
