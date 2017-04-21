using Newtonsoft.Json;

namespace Hangfire.AzureDocumentDB.Entities
{
    internal class Set : DocumentEntity
    {
        [JsonProperty("key")]
        public string Key { get; set; }

        [JsonProperty("value")]
        public string Value { get; set; }

        [JsonProperty("score")]
        public double? Score { get; set; }
    }
}