using Processor.Core.Messages;

namespace Processor.Core.Building;

/// <summary>
/// The one builder each domain must supply: it validates and transforms that domain's payload.
/// This is the seam where domain-specific processing lives — Core orchestrates it exactly like the
/// shared field builders (including its ability to dead-letter or drop a message), but never knows
/// what it does.
/// </summary>
public interface IDomainDataBuilder<in TInput, TDomainData> : IOutputFieldBuilder<TInput, TDomainData>
    where TInput : InputMessage<TDomainData>
    where TDomainData : class, IDomainData, new()
{
}
