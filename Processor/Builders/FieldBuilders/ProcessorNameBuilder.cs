using Processor.Builders.Core;
using Processor.Messages;

namespace Processor.Builders.FieldBuilders;

public class ProcessorNameBuilder : IOutputFieldBuilder<string>
{
    public FieldBuildResult<string> Build(InputMessage input)
    {
        return FieldBuildResult<string>.Ok(Environment.MachineName);
    }
}