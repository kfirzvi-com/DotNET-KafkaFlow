using Processor.Domain.Building;
using Processor.Domain.Messages;

namespace Processor.Domain.Building.FieldBuilders;

public class ProcessedAtBuilder : IOutputFieldBuilder<DateTime>
{
    public FieldBuildResult<DateTime> Build(InputMessage input)
    {
        var timestamp = input.Timestamp == default ? DateTime.UtcNow : input.Timestamp;
        return FieldBuildResult<DateTime>.Ok(timestamp);
    }
}