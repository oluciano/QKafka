using QKafka.Tests.Support;

namespace QKafka.Tests.Sagas;

public sealed class SagaUnitTests
{
    [Fact]
    public void NewSaga_Created_HasAnEmptyCorrelationIdAndAFreshState()
    {
        var sut = new SampleSaga();

        Assert.Equal(Guid.Empty, sut.CorrelationId);
        Assert.NotNull(sut.State);
        Assert.Equal("none", sut.State.Step);
        Assert.Equal(0, sut.State.Total);
    }

    [Fact]
    public void CorrelationIdAndState_Assigned_AreReadBack()
    {
        var id = Guid.NewGuid();
        var state = new SampleSagaState { Step = "paid", Total = 10 };

        var sut = new SampleSaga { CorrelationId = id, State = state };

        Assert.Equal(id, sut.CorrelationId);
        Assert.Same(state, sut.State);
    }

    [Fact]
    public void TwoSagas_Created_DoNotShareState()
    {
        var first = new SampleSaga();
        var second = new SampleSaga();

        first.State.Total = 5;

        Assert.NotSame(first.State, second.State);
        Assert.Equal(0, second.State.Total);
    }
}
