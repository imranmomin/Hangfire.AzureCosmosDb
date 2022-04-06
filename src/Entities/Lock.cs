using System;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;

// ReSharper disable once CheckNamespace
namespace Hangfire.Azure.Documents;

internal class Lock : DocumentBase
{
	[JsonProperty("last_heartbeat")]
	[JsonConverter(typeof(UnixDateTimeConverter))]
	public DateTime? LastHeartBeat { get; set; }

	public override DocumentTypes DocumentType => DocumentTypes.Lock;
}