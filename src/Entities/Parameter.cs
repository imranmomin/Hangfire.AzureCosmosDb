using Newtonsoft.Json;

// ReSharper disable once CheckNamespace
namespace Hangfire.Azure.Documents
{
    public class Parameter
    {
        [JsonProperty("name")]
        public string Name { get; set; } = null!;

        [JsonProperty("value")]
        public string Value { get; set; } = null!;
    }
}