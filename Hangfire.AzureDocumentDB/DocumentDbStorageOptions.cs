using Microsoft.Azure.Documents.Client;
using System;

// ReSharper disable MemberCanBePrivate.Global
// ReSharper disable AutoPropertyCanBeMadeGetOnly.Global
namespace Hangfire.Azure
{
    /// <summary>
    /// Options for DocumentDbStorage
    /// </summary>
    public class DocumentDbStorageOptions
    {
        internal string DatabaseName { get; set; }
        internal string CollectionName { get; set; }

        /// <summary>
        /// Get or sets the request timemout for DocumentDB client. Default value set to 30 seconds
        /// </summary>
        public TimeSpan RequestTimeout { get; set; }

        /// <summary>
        /// Get or set the interval timespan to process expired enteries. Default value 15 minutes
        /// Expired items under "locks", "jobs", "lists", "sets", "hashs", "counters/aggregrated" will be checked 
        /// </summary>
        public TimeSpan ExpirationCheckInterval { get; set; }

        /// <summary>
        /// Get or sets the interval timespan to aggreated the counters. Default value 1 minute
        /// </summary>
        public TimeSpan CountersAggregateInterval { get; set; }

        /// <summary>
        /// Gets or sets the interval timespan to poll the queue for processing any new jobs. Default value 2 minutes
        /// </summary>
        public TimeSpan QueuePollInterval { get; set; }

        /// <summary>
        /// Gets or sets the connection mode for the DocumentDB client. Default value is Direct.
        /// </summary>
        public ConnectionMode ConnectionMode { get; set; }

        /// <summary>
        /// Gets or sets the connection protocol for the DocumentDB client. Default value is TCP.
        /// </summary>
        public Protocol ConnectionProtocol { get; set; }

        /// <summary>
        /// Create an instance of AzureDocumentDB Storage option with default values
        /// </summary>
        public DocumentDbStorageOptions()
        {
            RequestTimeout = TimeSpan.FromSeconds(30);
            ExpirationCheckInterval = TimeSpan.FromMinutes(2);
            CountersAggregateInterval = TimeSpan.FromMinutes(1);
            QueuePollInterval = TimeSpan.FromSeconds(2);
            ConnectionMode = ConnectionMode.Direct;
            ConnectionProtocol = Protocol.Tcp;
        }
    }
}
