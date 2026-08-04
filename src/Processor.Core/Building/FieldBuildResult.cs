namespace Processor.Core.Building;

/// <summary>
/// Result of computing a single output field. <typeparamref name="T"/> is the field's own type — it
/// is unrelated to the domain payload, so a domain data builder simply returns
/// <c>FieldBuildResult&lt;TDomainData&gt;</c>.
/// </summary>
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
