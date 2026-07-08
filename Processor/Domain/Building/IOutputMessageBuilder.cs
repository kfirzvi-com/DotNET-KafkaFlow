using Processor.Domain.Messages;

namespace Processor.Domain.Building;

public interface IOutputMessageBuilder
{
    /// <summary>
    /// Builds the output outcome for a message. <paramref name="dataTypeId"/> (from the Kafka
    /// header/key) is filtered against the data-type settings first: inactive or unknown data
    /// types short-circuit to <see cref="BuildStatus.Filtered"/>.
    /// </summary>
    Task<BuildOutcome> Build(InputMessage input, string? dataTypeId, CancellationToken cancellationToken = default);
}
