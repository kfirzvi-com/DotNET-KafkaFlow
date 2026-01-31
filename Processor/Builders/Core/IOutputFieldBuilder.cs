using Processor.Messages;

namespace Processor.Builders.Core;

public interface IOutputFieldBuilder<T>
{
    FieldBuildResult<T> Build(InputMessage input);
}
