using Processor.Builders.Core;
using Processor.Messages;

namespace Processor.Builders.FieldBuilders;

public class OutputIdBuilder : IOutputFieldBuilder<string>
{
    public FieldBuildResult<string> Build(InputMessage input)
    {
        if (string.IsNullOrWhiteSpace(input.Id))
        {
            return FieldBuildResult<string>.DeadLetter("Missing message id");
        }

        return FieldBuildResult<string>.Ok(input.Id.Trim());
    }
}