using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using Hangfire.Azure.Queue;
using Hangfire.Logging;
using Hangfire.Server;
using Hangfire.Storage;

using Microsoft.Azure.Cosmos;

using Newtonsoft.Json;
using Newtonsoft.Json.Serialization;


namespace Hangfire.Azure
{
    /// <summary>
    /// CosmosDbStorage extend the storage option for Hangfire.
    /// </summary>
    [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1001:TypesThatOwnDisposableFieldsShouldBeDisposable")]
    public sealed class CosmosDbStorage : JobStorage
    {
        internal PersistentJobQueueProviderCollection QueueProviders { get; }

        internal CosmosDbStorageOptions StorageOptions { get; }

        private CosmosClient Client { get; }

        internal Container Container { get; private set; }

        private readonly string database;
        private readonly string collection;
        private readonly JsonSerializerSettings settings = new JsonSerializerSettings
        {
            NullValueHandling = NullValueHandling.Ignore,
            DateTimeZoneHandling = DateTimeZoneHandling.Utc,
            ContractResolver = new CamelCasePropertyNamesContractResolver
            {
                NamingStrategy = new CamelCaseNamingStrategy(false, false)
            }
        };

        /// <summary>
        /// Initializes the CosmosDbStorage form the url auth secret provide.
        /// </summary>
        /// <param name="url">The url string to Cosmos Database</param>
        /// <param name="authSecret">The secret key for the Cosmos Database</param>
        /// <param name="database">The name of the database to connect with</param>
        /// <param name="collection">The name of the collection/container on the database</param>
        /// <param name="options">The CosmosDbStorageOptions object to override any of the options</param>
        /// <param name="storageOptions"></param>
        public CosmosDbStorage(string url, string authSecret, string database, string collection, CosmosClientOptions options = null, CosmosDbStorageOptions storageOptions = null)
        {
            this.database = database;
            this.collection = collection;
            StorageOptions = storageOptions ?? new CosmosDbStorageOptions();

            JobQueueProvider provider = new JobQueueProvider(this);
            QueueProviders = new PersistentJobQueueProviderCollection(provider);

            options = options ?? new CosmosClientOptions();
            options.ApplicationName = "Hangfire";
            options.Serializer = new CosmosJsonSerializer(settings);
            Client = new CosmosClient(url, authSecret, options);
            Initialize();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public override IStorageConnection GetConnection() => new CosmosDbConnection(this);

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public override IMonitoringApi GetMonitoringApi() => new CosmosDbMonitoringApi(this);

#pragma warning disable 618
        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public override IEnumerable<IServerComponent> GetComponents()
#pragma warning restore 618
        {
            yield return new ExpirationManager(this);
            yield return new CountersAggregator(this);
        }

        /// <summary>
        /// Prints out the storage options
        /// </summary>
        /// <param name="logger"></param>
        public override void WriteOptionsToLog(ILog logger)
        {
            logger.Info("Using the following options for Azure Cosmos DB job storage:");
            logger.Info($"     Cosmos DB Url: {Client.Endpoint.AbsoluteUri}");
            logger.Info($"     Request Timeout: {Client.ClientOptions.RequestTimeout}");
            logger.Info($"     Counter Aggregate Interval: {StorageOptions.CountersAggregateInterval.TotalSeconds} seconds");
            logger.Info($"     Queue Poll Interval: {StorageOptions.QueuePollInterval.TotalSeconds} seconds");
            logger.Info($"     Expiration Check Interval: {StorageOptions.ExpirationCheckInterval.TotalSeconds} seconds");
        }

        /// <summary>
        /// Return the name of the database
        /// </summary>
        /// <returns></returns>
        public override string ToString() => $"Cosmos DB : {database}";

        private void Initialize()
        {
            ILog logger = LogProvider.For<CosmosDbStorage>();

            // create database
            logger.Info($"Creating database : {database}");
            Task<DatabaseResponse> databaseTask = Client.CreateDatabaseIfNotExistsAsync(database);

            // create document collection
            Task<ContainerResponse> containerTask = databaseTask.ContinueWith(t =>
            {
                logger.Info($"Creating document collection : {collection}");
                Database resultDatabase = t.Result.Database;
                return resultDatabase.CreateContainerIfNotExistsAsync(collection, "/type");
            }, TaskContinuationOptions.OnlyOnRanToCompletion).Unwrap();

            // create stored procedures 
            Task storedProcedureTask = containerTask.ContinueWith(t =>
            {
                Container = t.Result;
                System.Reflection.Assembly assembly = System.Reflection.Assembly.GetExecutingAssembly();
                string[] storedProcedureFiles = assembly.GetManifestResourceNames().Where(n => n.EndsWith(".js")).ToArray();
                foreach (string storedProcedureFile in storedProcedureFiles)
                {
                    logger.Info($"Creating storedprocedure : {storedProcedureFile}");
                    Stream stream = assembly.GetManifestResourceStream(storedProcedureFile);
                    using (MemoryStream memoryStream = new MemoryStream())
                    {
                        stream?.CopyTo(memoryStream);
                        Microsoft.Azure.Cosmos.Scripts.StoredProcedureProperties sp = new Microsoft.Azure.Cosmos.Scripts.StoredProcedureProperties
                        {
                            Body = Encoding.UTF8.GetString(memoryStream.ToArray()),
                            Id = Path.GetFileNameWithoutExtension(storedProcedureFile)?
                                .Split(new[] { '.' }, StringSplitOptions.RemoveEmptyEntries)
                                .Last()
                        };

                        Task<Microsoft.Azure.Cosmos.Scripts.StoredProcedureResponse> spTask = Container.Scripts.ReplaceStoredProcedureAsync(sp);
                        spTask.ContinueWith(x =>
                        {
                            if (x.Status == TaskStatus.Faulted && x.Exception.InnerException is CosmosException ex && ex.StatusCode == System.Net.HttpStatusCode.NotFound)
                            {
                                return Container.Scripts.CreateStoredProcedureAsync(sp);
                            }

                            return Task.FromResult(x.Result);
                        }).Unwrap().Wait();
                    }
                    stream?.Close();
                }
            }, TaskContinuationOptions.OnlyOnRanToCompletion);

            storedProcedureTask.Wait();
            if (storedProcedureTask.IsFaulted || storedProcedureTask.IsCanceled)
            {
                throw new ApplicationException("Unable to create the stored procedures", databaseTask.Exception);
            }
        }
    }
}
