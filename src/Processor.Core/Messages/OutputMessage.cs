namespace Processor.Core.Messages;

/// <summary>
/// Output message shared by every domain, parameterized by the domain's payload type so the
/// processed domain data travels alongside the shared processed fields. Closed per domain at the
/// producer registration (<c>OutputMessage&lt;PostData&gt;</c>), so each deployment serializes only
/// its own shape.
/// </summary>
public class OutputMessage<TDomainData>
    where TDomainData : class, IDomainData, new()
{
    public string Id { get; set; } = string.Empty;

    public string ProcessedContent { get; set; } = string.Empty;

    public DateTime ProcessedAt { get; set; }

    public string ProcessorName { get; set; } = string.Empty;

    /// <summary>Name of the domain that produced this message (from <see cref="IDomainData.DomainName"/>).</summary>
    public string Domain { get; set; } = string.Empty;

    /// <summary>The processed domain-specific payload, produced by the domain's data builder.</summary>
    public TDomainData DomainData { get; set; } = new();
}
