using Newtonsoft.Json;

namespace Hangfire.AzureDocumentDB.Entities
{
    internal class Counter : DocumentEntity
    {
        [JsonProperty("key")]
        public string Key { get; set; }

        [JsonProperty("value")]
        public int Value { get; set; }

        [JsonProperty("country_type")]
        public CounterTypes Type { get; set; }
    }

    internal enum CounterTypes
    {
        Raw = 1,
        Aggregrate = 2
    }
}
