using Newtonsoft.Json;

namespace Hangfire.AzureDocumentDB.Entities
{
    internal class Hash : DocumentEntity
    {
        [JsonProperty("key")]
        public string Key { get; set; }

        [JsonProperty("field")]
        public string Field { get; set; }

        [JsonProperty("value")]
        public string Value { get; set; }
    }
}