using QKafka.Sagas;

namespace QKafka.Tests.Support;

/// <summary>A minimal concrete saga so the abstract base class can be instantiated.</summary>
internal sealed class SampleSaga : Saga<SampleSagaState>
{
}
