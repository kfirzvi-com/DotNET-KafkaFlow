using Processor.Core.Messages;

namespace Processor.Core.Building;

/// <summary>
/// Computes one field of the output message from the input.
/// <para>
/// <typeparamref name="TInput"/> is contravariant, which is what lets Core's shared builders be
/// declared once against <see cref="IInputMessage"/> and still satisfy a domain's
/// <c>IOutputFieldBuilder&lt;PostInputMessage, string&gt;</c> dependency — no per-domain copies of
/// the shared field logic.
/// </para>
/// </summary>
public interface IOutputFieldBuilder<in TInput, TValue>
    where TInput : IInputMessage
{
    FieldBuildResult<TValue> Build(TInput input);
}
