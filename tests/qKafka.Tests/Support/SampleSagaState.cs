namespace QKafka.Tests.Support;

/// <summary>A sample saga state.</summary>
internal sealed class SampleSagaState
{
    public string Step { get; set; } = "none";

    public int Total { get; set; }
}
