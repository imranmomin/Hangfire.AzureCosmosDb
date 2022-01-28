using System;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;

// ReSharper disable once CheckNamespace
namespace Hangfire.Azure.Documents;

public class Set : DocumentBase
{
	[JsonProperty("key")]
	public string Key { get; set; } = null!;

	[JsonProperty("value")]
	public string Value { get; set; } = null!;

	[JsonProperty("score")]
	public double Score { get; set; }

	[JsonProperty("created_on")]
	[JsonConverter(typeof(UnixDateTimeConverter))]
	public DateTime CreatedOn { get; set; }

	public override DocumentTypes DocumentType => DocumentTypes.Set;
}