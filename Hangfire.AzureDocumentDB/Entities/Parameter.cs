using Newtonsoft.Json;

namespace Hangfire.AzureDocumentDB.Entities
{
    internal class Parameter : DocumentEntity
    {
        [JsonProperty("name")]
        public string Name { get; set; }

        [JsonProperty("value")]
        public string Value { get; set; }
    }
}