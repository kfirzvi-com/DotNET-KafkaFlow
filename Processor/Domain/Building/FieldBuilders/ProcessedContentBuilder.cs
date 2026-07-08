using Processor.Domain.Building;
using Processor.Domain.Messages;

namespace Processor.Domain.Building.FieldBuilders;

public class ProcessedContentBuilder : IOutputFieldBuilder<string>
{
    public FieldBuildResult<string> Build(InputMessage input)
    {
        if (string.IsNullOrWhiteSpace(input.Content))
        {
            return FieldBuildResult<string>.Drop("Content is empty");
        }

        return FieldBuildResult<string>.Ok(input.Content.ToUpperInvariant());
    }
}