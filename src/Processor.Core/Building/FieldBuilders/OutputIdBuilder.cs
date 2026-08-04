using Processor.Core.Messages;

namespace Processor.Core.Building.FieldBuilders;

/// <summary>
/// Shared across all domains: declared against <see cref="IInputMessage"/> so one registration
/// serves every domain's concrete message type.
/// </summary>
public class OutputIdBuilder : IOutputFieldBuilder<IInputMessage, string>
{
    public FieldBuildResult<string> Build(IInputMessage input)
    {
        if (string.IsNullOrWhiteSpace(input.Id))
        {
            return FieldBuildResult<string>.DeadLetter("Missing message id");
        }

        return FieldBuildResult<string>.Ok(input.Id.Trim());
    }
}
