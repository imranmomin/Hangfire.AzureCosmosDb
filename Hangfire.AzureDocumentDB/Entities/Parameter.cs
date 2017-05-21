using Newtonsoft.Json;

namespace Hangfire.AzureDocumentDB.Entities
{
    internal class Parameter
    {
        [JsonProperty("name")]
        public string Name { get; set; }

        [JsonProperty("value")]
        public string Value { get; set; }
    }
}