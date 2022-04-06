using System.Linq;
using System.Threading.Tasks;
using Hangfire.Azure.Documents;
using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Cosmos.Scripts;

namespace Hangfire.Azure.Helper;

internal static class StoredprocedureHelper
{
	// ReSharper disable once UnusedMethodReturnValue.Global
	internal static int ExecuteUpsertDocuments<T>(this Container container, Data<T> data, PartitionKey partitionKey)
	{
		int affected = 0;
		Data<T> records = new(data.Items);

		do
		{
			records.Items = data.Items.Skip(affected).ToList();
			Task<StoredProcedureExecuteResponse<int>> task = container.Scripts.ExecuteStoredProcedureAsync<int>("upsertDocuments", partitionKey, new dynamic[] { records });

			int result = task
				.ExecuteWithRetriesAsync()
				.ExecuteSynchronously();

			affected += result;

		} while (affected < data.Items.Count);

		return affected;
	}

	internal static int ExecuteDeleteDocuments(this Container container, string query, PartitionKey partitionKey)
	{
		int affected = 0;
		ProcedureResponse response;

		do
		{
			Task<StoredProcedureExecuteResponse<ProcedureResponse>> task = container.Scripts.ExecuteStoredProcedureAsync<ProcedureResponse>("deleteDocuments", partitionKey, new dynamic[] { query });

			response = task
				.ExecuteWithRetriesAsync()
				.ExecuteSynchronously();

			affected += response.Affected;

		} while (response.Continuation);

		return affected;
	}

	internal static void ExecutePersistDocuments(this Container container, string query, PartitionKey partitionKey)
	{
		ProcedureResponse response;

		do
		{
			Task<StoredProcedureExecuteResponse<ProcedureResponse>> task = container.Scripts.ExecuteStoredProcedureAsync<ProcedureResponse>("persistDocuments", partitionKey, new dynamic[] { query });

			response = task
				.ExecuteWithRetriesAsync()
				.ExecuteSynchronously();

		} while (response.Continuation);
	}

	internal static void ExecuteExpireDocuments(this Container container, string query, int epoch, PartitionKey partitionKey)
	{
		ProcedureResponse response;

		do
		{
			Task<StoredProcedureExecuteResponse<ProcedureResponse>> task = container.Scripts.ExecuteStoredProcedureAsync<ProcedureResponse>("expireDocuments", partitionKey, new dynamic[] { query, epoch });

			response = task
				.ExecuteWithRetriesAsync()
				.ExecuteSynchronously();

		} while (response.Continuation);
	}
}