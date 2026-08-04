using Processor.Core.Messages;

namespace Processor.Core.Building.FieldBuilders;

/// <summary>Shared across all domains: stamps the processing time.</summary>
public class ProcessedAtBuilder : IOutputFieldBuilder<IInputMessage, DateTime>
{
    public FieldBuildResult<DateTime> Build(IInputMessage input)
    {
        var timestamp = input.Timestamp == default ? DateTime.UtcNow : input.Timestamp;
        return FieldBuildResult<DateTime>.Ok(timestamp);
    }
}
