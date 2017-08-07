using Newtonsoft.Json;

namespace Hangfire.Azure.Documents
{
    internal class Lock : DocumentBase
    {
        [JsonProperty("name")]
        public string Name { get; set; }

        public override DocumentTypes DocumentType => DocumentTypes.Lock;
    }
}
