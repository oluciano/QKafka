using System.Threading.Tasks;

namespace QKafka;

/// <summary>
/// A delegate representing the consumer pipeline execution.
/// </summary>
/// <param name="context">The execution context.</param>
/// <returns>A task representing the asynchronous operation.</returns>
internal delegate Task KafkaConsumerDelegate(IKafkaContext context);
