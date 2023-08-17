using System;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;

// ReSharper disable once CheckNamespace
namespace Hangfire.Azure.Documents;

internal class Server : DocumentBase
{
    [JsonProperty("workers")]
    public int Workers { get; set; }

    [JsonProperty("queues")]
    public string[] Queues { get; set; } = null!;

    [JsonProperty("created_on")]
    [JsonConverter(typeof(UnixDateTimeConverter))]
    public DateTime CreatedOn { get; set; }

    [JsonProperty("last_heartbeat")]
    [JsonConverter(typeof(UnixDateTimeConverter))]
    public DateTime LastHeartbeat { get; set; }

    public override DocumentTypes DocumentType => DocumentTypes.Server;
}