using System;
using System.Collections.Generic;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;

// ReSharper disable once CheckNamespace
namespace Hangfire.Azure.Documents;

public class State : DocumentBase
{
	[JsonProperty("job_id")]
	public string JobId { get; set; } = null!;

	[JsonProperty("name")]
	public string Name { get; set; } = null!;

	[JsonProperty("reason")]
	public string Reason { get; set; } = null!;

	[JsonProperty("created_on")]
	[JsonConverter(typeof(UnixDateTimeConverter))]
	public DateTime CreatedOn { get; set; }

	[JsonProperty("data")]
	public Dictionary<string, string> Data { get; set; } = null!;

	public override DocumentTypes DocumentType => DocumentTypes.State;
}