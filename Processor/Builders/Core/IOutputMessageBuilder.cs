using Processor.Messages;

namespace Processor.Builders.Core;

public interface IOutputMessageBuilder
{
    BuildOutcome Build(InputMessage input);
}
