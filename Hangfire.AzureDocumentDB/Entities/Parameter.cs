using Newtonsoft.Json;

namespace Hangfire.Azure.Documents
{
    internal class Parameter
    {
        [JsonProperty("name")]
        public string Name { get; set; }

        [JsonProperty("value")]
        public string Value { get; set; }
    }
}