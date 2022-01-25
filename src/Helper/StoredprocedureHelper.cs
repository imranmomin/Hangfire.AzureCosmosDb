using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Hangfire.Azure.Documents;
using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Cosmos.Scripts;

namespace Hangfire.Azure.Helper;

public static class StoredprocedureHelper
{
	// ReSharper disable once UnusedMethodReturnValue.Global
	public static int ExecuteUpsertDocuments<T>(this Container container, Data<T> data, PartitionKey partitionKey, CancellationToken cancellationToken = default)
	{
		int affected = 0;
		Data<T> records = new(data.Items);
		do
		{
			records.Items = data.Items.Skip(affected).ToList();
			Task<StoredProcedureExecuteResponse<int>> task = container.Scripts.ExecuteStoredProcedureAsync<int>("upsertDocuments", partitionKey, new dynamic[] { records }, cancellationToken: cancellationToken);
			task.Wait(cancellationToken);
			affected += task.Result.Resource;
		} while (affected < data.Items.Count);
		return affected;
	}

	public static int ExecuteDeleteDocuments(this Container container, string query, PartitionKey partitionKey, CancellationToken cancellationToken = default)
	{
		int affected = 0;
		ProcedureResponse response;
		do
		{
			Task<StoredProcedureExecuteResponse<ProcedureResponse>> task = container.Scripts.ExecuteStoredProcedureAsync<ProcedureResponse>("deleteDocuments", partitionKey, new dynamic[] { query }, cancellationToken: cancellationToken);
			task.Wait(cancellationToken);
			response = task.Result;
			affected += response.Affected;
		} while (response.Continuation);
		return affected;
	}

	public static void ExecutePersistDocuments(this Container container, string query, PartitionKey partitionKey, CancellationToken cancellationToken = default)
	{
		ProcedureResponse response;
		do
		{
			Task<StoredProcedureExecuteResponse<ProcedureResponse>> task = container.Scripts.ExecuteStoredProcedureAsync<ProcedureResponse>("persistDocuments", partitionKey, new dynamic[] { query }, cancellationToken: cancellationToken);
			task.Wait(cancellationToken);
			response = task.Result;
		} while (response.Continuation);
	}

	public static void ExecuteExpireDocuments(this Container container, string query, int epoch, PartitionKey partitionKey, CancellationToken cancellationToken = default)
	{
		ProcedureResponse response;
		do
		{
			Task<StoredProcedureExecuteResponse<ProcedureResponse>> task = container.Scripts.ExecuteStoredProcedureAsync<ProcedureResponse>("expireDocuments", partitionKey, new dynamic[] { query, epoch }, cancellationToken: cancellationToken);
			task.Wait(cancellationToken);
			response = task.Result;
		} while (response.Continuation);
	}
}