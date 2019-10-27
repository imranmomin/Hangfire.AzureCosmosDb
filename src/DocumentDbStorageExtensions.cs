using System;
using Hangfire.Azure;

// ReSharper disable UnusedMember.Global
// ReSharper disable once CheckNamespace
namespace Hangfire
{
    /// <summary>
    /// Extension methods to user DocumentDb Storage.
    /// </summary>
    public static class DocumentDbStorageExtensions
    {
       /// <summary>
        /// Enables to attach Azure DocumentDb to Hangfire
        /// </summary>
        /// <param name="configuration">The IGlobalConfiguration object</param>
        /// <param name="url">The url string to DocumentDb Database</param>
        /// <param name="authSecret">The secret key for the DocumentDb Database</param>
        /// <param name="database">The name of the database to connect with</param>
        /// <param name="collection">The name of the collection on the database</param>
        /// <param name="options">The DocumentDbStorage object to override any of the options</param>
        /// <returns></returns>
        public static IGlobalConfiguration<DocumentDbStorage> UseAzureDocumentDbStorage(this IGlobalConfiguration configuration, string url, string authSecret, string database, string collection, DocumentDbStorageOptions options = null)
        {
            if (configuration == null) throw new ArgumentNullException(nameof(configuration));
            if (string.IsNullOrEmpty(url)) throw new ArgumentNullException(nameof(url));
            if (string.IsNullOrEmpty(authSecret)) throw new ArgumentNullException(nameof(authSecret));
            
            DocumentDbStorage storage = new DocumentDbStorage(url, authSecret, database, collection, options);
            return configuration.UseStorage(storage);
        }
    }
}
