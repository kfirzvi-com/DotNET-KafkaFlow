using Processor.Core.Messages;

namespace Processor.Core.Building;

/// <summary>
/// Final outcome of building one message, parameterized by the domain payload so a successful
/// outcome carries that domain's fully-typed <see cref="OutputMessage{TDomainData}"/>.
/// </summary>
public class BuildOutcome<TDomainData>
    where TDomainData : class, IDomainData, new()
{
    private BuildOutcome(BuildStatus status, OutputMessage<TDomainData>? message, string? reason)
    {
        Status = status;
        Message = message;
        Reason = reason;
    }

    public BuildStatus Status { get; }
    public OutputMessage<TDomainData>? Message { get; }
    public string? Reason { get; }

    public static BuildOutcome<TDomainData> Ok(OutputMessage<TDomainData> message) =>
        new(BuildStatus.Ok, message, null);

    public static BuildOutcome<TDomainData> DeadLetter(string reason) =>
        new(BuildStatus.DeadLetter, null, reason);

    public static BuildOutcome<TDomainData> Drop(string reason) =>
        new(BuildStatus.Drop, null, reason);

    public static BuildOutcome<TDomainData> Filtered(string reason) =>
        new(BuildStatus.Filtered, null, reason);
}
