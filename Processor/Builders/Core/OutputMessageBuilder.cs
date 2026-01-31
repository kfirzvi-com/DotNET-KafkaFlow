using Processor.Builders.FieldBuilders;
using Processor.Messages;

namespace Processor.Builders.Core;

public class OutputMessageBuilder : IOutputMessageBuilder
{
    private readonly OutputIdBuilder _idBuilder;
    private readonly ProcessedContentBuilder _contentBuilder;
    private readonly ProcessedAtBuilder _processedAtBuilder;
    private readonly ProcessorNameBuilder _processorNameBuilder;

    public OutputMessageBuilder(
        OutputIdBuilder idBuilder,
        ProcessedContentBuilder contentBuilder,
        ProcessedAtBuilder processedAtBuilder,
        ProcessorNameBuilder processorNameBuilder)
    {
        _idBuilder = idBuilder;
        _contentBuilder = contentBuilder;
        _processedAtBuilder = processedAtBuilder;
        _processorNameBuilder = processorNameBuilder;
    }

    public BuildOutcome Build(InputMessage input)
    {
        var idResult = _idBuilder.Build(input);
        if (idResult.Status != BuildStatus.Ok)
        {
            return ToOutcome(idResult);
        }

        var contentResult = _contentBuilder.Build(input);
        if (contentResult.Status != BuildStatus.Ok)
        {
            return ToOutcome(contentResult);
        }

        var processedAtResult = _processedAtBuilder.Build(input);
        if (processedAtResult.Status != BuildStatus.Ok)
        {
            return ToOutcome(processedAtResult);
        }

        var processorNameResult = _processorNameBuilder.Build(input);
        if (processorNameResult.Status != BuildStatus.Ok)
        {
            return ToOutcome(processorNameResult);
        }

        var message = new OutputMessage
        {
            Id = idResult.Value ?? string.Empty,
            ProcessedContent = contentResult.Value ?? string.Empty,
            ProcessedAt = processedAtResult.Value,
            ProcessorName = processorNameResult.Value ?? string.Empty
        };

        return BuildOutcome.Ok(message);
    }

    private static BuildOutcome ToOutcome<T>(FieldBuildResult<T> result)
    {
        var reason = result.Reason ?? "Unknown reason";

        return result.Status switch
        {
            BuildStatus.DeadLetter => BuildOutcome.DeadLetter(reason),
            BuildStatus.Drop => BuildOutcome.Drop(reason),
            _ => BuildOutcome.DeadLetter("Unexpected builder status")
        };
    }
}