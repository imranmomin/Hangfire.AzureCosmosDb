using Newtonsoft.Json;

namespace Hangfire.Azure.Documents
{
    internal class Hash : DocumentBase
    {
        [JsonProperty("key")]
        public string Key { get; set; }

        [JsonProperty("field")]
        public string Field { get; set; }

        [JsonProperty("value")]
        public string Value { get; set; }

        public override DocumentTypes DocumentType => DocumentTypes.Hash;
    }
}