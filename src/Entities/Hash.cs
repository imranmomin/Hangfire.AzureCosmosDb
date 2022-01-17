using Newtonsoft.Json;

// ReSharper disable once CheckNamespace
namespace Hangfire.Azure.Documents
{
    public class Hash : DocumentBase
    {
        [JsonProperty("key")]
        public string Key { get; set; } = null!;

        [JsonProperty("field")]
        public string Field { get; set; } = null!;

        [JsonProperty("value")]
        public string? Value { get; set; }

        public override DocumentTypes DocumentType => DocumentTypes.Hash;
    }
}