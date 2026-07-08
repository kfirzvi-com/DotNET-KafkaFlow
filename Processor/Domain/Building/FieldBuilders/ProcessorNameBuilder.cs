using Processor.Domain.Building;
using Processor.Domain.Messages;

namespace Processor.Domain.Building.FieldBuilders;

public class ProcessorNameBuilder : IOutputFieldBuilder<string>
{
    public FieldBuildResult<string> Build(InputMessage input)
    {
        return FieldBuildResult<string>.Ok(Environment.MachineName);
    }
}