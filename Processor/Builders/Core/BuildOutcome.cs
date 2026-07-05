using Processor.Messages;

namespace Processor.Builders.Core;

public class BuildOutcome
{
    private BuildOutcome(BuildStatus status, OutputMessage? message, string? reason)
    {
        Status = status;
        Message = message;
        Reason = reason;
    }

    public BuildStatus Status { get; }
    public OutputMessage? Message { get; }
    public string? Reason { get; }

    public static BuildOutcome Ok(OutputMessage message) => new(BuildStatus.Ok, message, null);

    public static BuildOutcome DeadLetter(string reason) => new(BuildStatus.DeadLetter, null, reason);

    public static BuildOutcome Drop(string reason) => new(BuildStatus.Drop, null, reason);

    public static BuildOutcome Filtered(string reason) => new(BuildStatus.Filtered, null, reason);
}