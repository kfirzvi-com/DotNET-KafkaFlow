using Processor.Core.Messages;

namespace Processor.Domains.Profiles;

/// <summary>
/// The profiles domain's input message: the shared <see cref="InputMessage{TDomainData}"/> closed over
/// <see cref="ProfileData"/>, so <c>DomainData</c> on this type <em>is</em> the profile payload.
/// </summary>
public sealed class ProfileInputMessage : InputMessage<ProfileData>
{
}
