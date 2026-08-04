using Processor.Core.Messages;

namespace Processor.Core.Building;

/// <summary>
/// Builds a domain's output message. Parameterized over both the domain's input type and its payload
/// type so DI resolves exactly one closed builder per deployment — the domain selected in
/// <c>appsettings.json</c>.
/// </summary>
public interface IOutputMessageBuilder<in TInput, TDomainData>
    where TInput : InputMessage<TDomainData>
    where TDomainData : class, IDomainData, new()
{
    /// <summary>
    /// Builds the output outcome for a message. <paramref name="dataTypeId"/> (from the Kafka
    /// header/key) is filtered against the data-type settings first: inactive or unknown data
    /// types short-circuit to <see cref="BuildStatus.Filtered"/>.
    /// </summary>
    Task<BuildOutcome<TDomainData>> Build(
        TInput input, string? dataTypeId, CancellationToken cancellationToken = default);
}
