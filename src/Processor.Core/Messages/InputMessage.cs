namespace Processor.Core.Messages;

/// <summary>
/// Base input message shared by every domain, parameterized by the domain's payload type. A domain
/// inherits it and closes <typeparamref name="TDomainData"/>, which is what gives that domain's
/// message its own strongly-typed data field (post data, profile data, ...):
/// <code>
/// public sealed class PostInputMessage : InputMessage&lt;PostData&gt; { }
/// </code>
/// The shared fields (<see cref="Id"/>, <see cref="Content"/>, <see cref="Timestamp"/>) are handled
/// by Core; <see cref="DomainData"/> is handled by the domain project.
/// </summary>
public abstract class InputMessage<TDomainData> : IInputMessage
    where TDomainData : class, IDomainData, new()
{
    public string Id { get; set; } = string.Empty;

    public string Content { get; set; } = string.Empty;

    public DateTime Timestamp { get; set; }

    /// <summary>The domain-specific payload. Never null: defaults to an empty instance.</summary>
    public TDomainData DomainData { get; set; } = new();
}
