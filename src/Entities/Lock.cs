using Newtonsoft.Json;

// ReSharper disable once CheckNamespace
namespace Hangfire.Azure.Documents
{
    internal class Lock : DocumentBase
    {
        [JsonProperty("name")]
        public string Name { get; set; }

        [JsonProperty(PropertyName = "ttl", NullValueHandling = NullValueHandling.Ignore)]
        public int? TimeToLive { get; set; }

        public override DocumentTypes DocumentType => DocumentTypes.Lock;
    }
}
