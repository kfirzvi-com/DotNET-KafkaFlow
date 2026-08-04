using Processor.Core.Messages;

namespace Processor.Core.Building.FieldBuilders;

/// <summary>Shared across all domains: identifies the machine that processed the message.</summary>
public class ProcessorNameBuilder : IOutputFieldBuilder<IInputMessage, string>
{
    public FieldBuildResult<string> Build(IInputMessage input)
    {
        return FieldBuildResult<string>.Ok(Environment.MachineName);
    }
}
