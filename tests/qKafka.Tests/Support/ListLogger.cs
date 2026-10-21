using Microsoft.Extensions.Logging;

namespace QKafka.Tests.Support;

/// <summary>An <see cref="ILogger{T}"/> that keeps every entry so tests can assert on levels and structured values.</summary>
/// <typeparam name="T">The category type of the logger.</typeparam>
internal sealed class ListLogger<T> : ILogger<T>
{
    public List<(LogLevel Level, IReadOnlyList<KeyValuePair<string, object?>> State)> Entries { get; } = new();

    public List<object> Scopes { get; } = new();

    public IDisposable? BeginScope<TState>(TState state)
        where TState : notnull
    {
        Scopes.Add(state);
        return null;
    }

    public bool IsEnabled(LogLevel logLevel) => true;

    public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
    {
        var values = state as IReadOnlyList<KeyValuePair<string, object?>> ?? Array.Empty<KeyValuePair<string, object?>>();
        Entries.Add((logLevel, values));
    }

    public IEnumerable<double> LoggedDelays() =>
        Entries
            .Where(e => e.Level == LogLevel.Warning)
            .Select(e => Convert.ToDouble(e.State.First(kv => kv.Key == "Delay").Value, System.Globalization.CultureInfo.InvariantCulture));
}
