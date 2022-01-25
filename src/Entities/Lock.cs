using System;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;

// ReSharper disable once CheckNamespace
namespace Hangfire.Azure.Documents;

public class Lock : DocumentBase
{
	[JsonProperty("name")]
	public string Name { get; set; } = null!;

	[JsonProperty("last_heartbeat")]
	[JsonConverter(typeof(UnixDateTimeConverter))]
	public DateTime? LastHeartBeat { get; set; }

	public override DocumentTypes DocumentType => DocumentTypes.Lock;
}