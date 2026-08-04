using Processor.Core.Messages;

namespace Processor.Domains.Posts;

/// <summary>
/// The posts domain's input message: the shared <see cref="InputMessage{TDomainData}"/> closed over
/// <see cref="PostData"/>, so <c>DomainData</c> on this type <em>is</em> the post payload. No shared
/// field is redeclared here — that is the point of the generic base.
/// </summary>
public sealed class PostInputMessage : InputMessage<PostData>
{
}
