namespace Processor.Domain.Building;

public class FieldBuildResult<T>
{
    private FieldBuildResult(BuildStatus status, T? value, string? reason)
    {
        Status = status;
        Value = value;
        Reason = reason;
    }

    public BuildStatus Status { get; }
    public T? Value { get; }
    public string? Reason { get; }

    public static FieldBuildResult<T> Ok(T value) => new(BuildStatus.Ok, value, null);

    public static FieldBuildResult<T> DeadLetter(string reason) => new(BuildStatus.DeadLetter, default, reason);

    public static FieldBuildResult<T> Drop(string reason) => new(BuildStatus.Drop, default, reason);
}