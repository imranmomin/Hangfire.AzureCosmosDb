using System;
using System.Linq;
using System.Collections.Generic;

namespace Hangfire.AzureDocumentDB.Entities
{
    internal class State : FireEntity
    {
        public string Name { get; set; }
        public string Reason { get; set; }
        public DateTime CreatedOn { get; set; }
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