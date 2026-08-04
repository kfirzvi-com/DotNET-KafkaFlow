namespace Processor.Core.Messages;

/// <summary>
/// Marker for a domain-specific payload carried by <see cref="InputMessage{TDomainData}"/> and
/// <see cref="OutputMessage{TDomainData}"/>. Each domain project defines exactly one implementation
/// (e.g. post data, profile data); Core never depends on the concrete shape.
/// </summary>
public interface IDomainData
{
    /// <summary>
    /// Stable name of the domain this payload belongs to (e.g. <c>posts</c>). Used for logging and
    /// metric tagging so a deployment's telemetry states which domain it is running.
    /// </summary>
    string DomainName { get; }
}
