using System.IO;
using System.Text;

using Newtonsoft.Json;
using Microsoft.Azure.Cosmos;

namespace Hangfire.Azure
{
    public class CosmosJsonSerializer : CosmosSerializer
    {
        private static readonly Encoding defaultEncoding = new UTF8Encoding(false, true);
        private readonly JsonSerializer serializer;

        public CosmosJsonSerializer() : this(new JsonSerializerSettings())
        {
        }

        public CosmosJsonSerializer(JsonSerializerSettings serializerSettings)
        {
            serializer = JsonSerializer.Create(serializerSettings);
        }

        public override T FromStream<T>(Stream stream)
        {
            using (stream)
            {
                if (typeof(Stream).IsAssignableFrom(typeof(T)))
                {
                    return (T)(object)stream;
                }

                using (StreamReader sr = new StreamReader(stream))
                {
                    using (JsonTextReader jsonTextReader = new JsonTextReader(sr))
                    {
                        return serializer.Deserialize<T>(jsonTextReader);
                    }
                }
            }
        }

        public override Stream ToStream<T>(T input)
        {
            MemoryStream streamPayload = new MemoryStream();
            using (StreamWriter streamWriter = new StreamWriter(streamPayload, defaultEncoding, 1024, true))
            {
                using (JsonWriter writer = new JsonTextWriter(streamWriter))
                {
                    writer.Formatting = Formatting.None;
                    serializer.Serialize(writer, input);
                    writer.Flush();
                    streamWriter.Flush();
                }
            }
            streamPayload.Position = 0;
            return streamPayload;
        }
    }
}
