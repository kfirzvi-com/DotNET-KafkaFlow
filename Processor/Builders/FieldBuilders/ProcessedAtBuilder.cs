using Processor.Builders.Core;
using Processor.Messages;

namespace Processor.Builders.FieldBuilders;

public class ProcessedAtBuilder : IOutputFieldBuilder<DateTime>
{
    public FieldBuildResult<DateTime> Build(InputMessage input)
    {
        var timestamp = input.Timestamp == default ? DateTime.UtcNow : input.Timestamp;
        return FieldBuildResult<DateTime>.Ok(timestamp);
    }
}