﻿using System;
using System.Collections.Generic;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;

// ReSharper disable once CheckNamespace
namespace Hangfire.Azure.Documents;

public abstract class DocumentBase
{
    [JsonProperty("id")] public string Id { get; set; } = Guid.NewGuid().ToString();

    [JsonProperty("_self")] public string? SelfLink { get; set; }

    [JsonProperty("expire_on")]
    [JsonConverter(typeof(UnixDateTimeConverter))]
    public DateTime? ExpireOn { get; set; }

    [JsonProperty(PropertyName = "ttl", NullValueHandling = NullValueHandling.Ignore)]
    public int? TimeToLive { get; set; }

    [JsonProperty("type")] public abstract DocumentTypes DocumentType { get; }

    [JsonProperty("_etag", NullValueHandling = NullValueHandling.Ignore)]
    public string? ETag { get; set; }
}

public enum DocumentTypes
{
    Server = 1,
    Job = 2,
    Queue = 3,
    Counter = 4,
    List = 5,
    Hash = 6,
    Set = 7,
    State = 8,
    Lock = 9
}

public class ProcedureResponse
{
    [JsonProperty("affected")] public int Affected { get; set; }

    [JsonProperty("continuation")] public bool Continuation { get; set; }
}

public class Data<T>
{
    public Data() => Items = new List<T>();

    public Data(List<T> items) => Items = items;

    [JsonProperty("items")] public List<T> Items { get; set; }
}