using System;
using System.Linq;
using System.Collections.Generic;
using Microsoft.Azure.Documents;
using Newtonsoft.Json;

namespace Hangfire.AzureDocumentDB.Entities
{
    internal class State : DocumentEntity
    {
        [JsonProperty("job_id")]
        public string JobId { get; set; }

        [JsonProperty("name")]
        public string Name { get; set; }

        [JsonProperty("reason")]
        public string Reason { get; set; }

        [JsonProperty("created_on")]
        [JsonConverter(typeof(UnixDateTimeConverter))]
        public DateTime CreatedOn { get; set; }
        
        [JsonProperty("data")]
        public Dictionary<string, string> Data { get; set; }
    }

    internal static class StateExtension
    {
        internal static Dictionary<string, string> Trasnform(this Dictionary<string, string> data) => data.ToDictionary(k => k.Key.ToProperCase(), v => v.Value);

        private static string ToProperCase(this string key)
        {
            string[] keys = key.Split(new[] { '_' }, StringSplitOptions.RemoveEmptyEntries)
                               .Select(k => System.Threading.Thread.CurrentThread.CurrentCulture.TextInfo.ToTitleCase(k))
                               .ToArray();
            return string.Join(string.Empty, keys);
        }
    }
}