namespace QKafka.Tests.Support;

/// <summary>A scoped service that records whether its scope was disposed.</summary>
internal sealed class DisposableProbe : IDisposable
{
    public bool Disposed { get; private set; }

    public void Dispose() => Disposed = true;
}
