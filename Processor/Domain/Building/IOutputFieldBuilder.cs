using Processor.Domain.Messages;

namespace Processor.Domain.Building;

public interface IOutputFieldBuilder<T>
{
    FieldBuildResult<T> Build(InputMessage input);
}
