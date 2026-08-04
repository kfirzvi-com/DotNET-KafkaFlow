namespace Processor.Core.Messages;

/// <summary>
/// Non-generic view of an input message: the fields every domain shares. Lets Core's shared field
/// builders (id, content, timestamp, processor name) be written once, against this interface, and
/// still apply to every domain's concrete message type via contravariance on
/// <see cref="Building.IOutputFieldBuilder{TInput, TValue}"/>.
/// </summary>
public interface IInputMessage
{
    string Id { get; }

    string Content { get; }

    DateTime Timestamp { get; }
}
