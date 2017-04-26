using Newtonsoft.Json;

namespace Hangfire.AzureDocumentDB.Entities
{
    internal class List : DocumentEntity
    {
        [JsonProperty("key")]
        public string Key { get; set; }

        [JsonProperty("value")]
        public string Value { get; set; }
    }
}
