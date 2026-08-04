namespace Processor.Core.Messages;

/// <summary>
/// Dead-letter envelope shared by every domain. Parameterized by the domain payload so the original
/// message is preserved with its domain data intact rather than flattened to the shared fields.
/// </summary>
public class DeadLetterMessage<TDomainData>
    where TDomainData : class, IDomainData, new()
{
    public string Id { get; set; } = string.Empty;

    public string Reason { get; set; } = string.Empty;

    /// <summary>Name of the domain that dead-lettered this message.</summary>
    public string Domain { get; set; } = string.Empty;

    /// <summary>Shared fields of the message that failed, plus its domain payload.</summary>
    public DeadLetterOriginal<TDomainData> OriginalMessage { get; set; } = new();

    public DateTime FailedAt { get; set; }
}

/// <summary>
/// Snapshot of the original input. A concrete type rather than <see cref="InputMessage{TDomainData}"/>
/// itself, because that base is abstract and each domain's subclass would otherwise leak into the
/// envelope's serialized shape.
/// </summary>
public class DeadLetterOriginal<TDomainData>
    where TDomainData : class, IDomainData, new()
{
    public string Id { get; set; } = string.Empty;

    public string Content { get; set; } = string.Empty;

    public DateTime Timestamp { get; set; }

    public TDomainData DomainData { get; set; } = new();

    /// <summary>Copies the shared fields and domain payload off an input message.</summary>
    public static DeadLetterOriginal<TDomainData> From(InputMessage<TDomainData> input) => new()
    {
        Id = input.Id,
        Content = input.Content,
        Timestamp = input.Timestamp,
        DomainData = input.DomainData
    };
}
