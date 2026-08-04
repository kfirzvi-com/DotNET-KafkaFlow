using Processor.Core.Messages;

namespace Processor.Core.Building.FieldBuilders;

/// <summary>Shared across all domains: uppercases the common content field.</summary>
public class ProcessedContentBuilder : IOutputFieldBuilder<IInputMessage, string>
{
    public FieldBuildResult<string> Build(IInputMessage input)
    {
        if (string.IsNullOrWhiteSpace(input.Content))
        {
            return FieldBuildResult<string>.Drop("Content is empty");
        }

        return FieldBuildResult<string>.Ok(input.Content.ToUpperInvariant());
    }
}
