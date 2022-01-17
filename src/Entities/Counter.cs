using Newtonsoft.Json;

// ReSharper disable once CheckNamespace
namespace Hangfire.Azure.Documents;

public class Counter : DocumentBase
{
    [JsonProperty("key")]
    public string Key { get; set; } = null!;

    [JsonProperty("value")]
    public int Value { get; set; }

    [JsonProperty("counterType")]
    public CounterTypes Type { get; set; }

    public override DocumentTypes DocumentType => DocumentTypes.Counter;
}

public enum CounterTypes
{
    Raw = 1,
    Aggregate = 2
}