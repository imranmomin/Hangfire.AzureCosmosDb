using Newtonsoft.Json;

// ReSharper disable once CheckNamespace
namespace Hangfire.Azure.Documents
{
    public class Lock : DocumentBase
    {
        [JsonProperty("name")]
        public string Name { get; set; } = null!;

        public override DocumentTypes DocumentType => DocumentTypes.Lock;
    }
}
