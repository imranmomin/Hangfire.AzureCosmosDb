using System;
using System.Collections;
using System.Collections.Generic;

namespace Hangfire.Azure.Queue;

internal sealed class PersistentJobQueueProviderCollection : IEnumerable<IPersistentJobQueueProvider>
{
	private readonly IPersistentJobQueueProvider provider;
	private readonly List<IPersistentJobQueueProvider> providers = new();
	private readonly Dictionary<string, IPersistentJobQueueProvider> providersByQueue = new(StringComparer.OrdinalIgnoreCase);

	public PersistentJobQueueProviderCollection(IPersistentJobQueueProvider provider)
	{
		this.provider = provider ?? throw new ArgumentNullException(nameof(provider));
		providers.Add(this.provider);
	}

	public IEnumerator<IPersistentJobQueueProvider> GetEnumerator() => providers.GetEnumerator();

	IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

	// ReSharper disable once UnusedMember.Global
	public void Add(IPersistentJobQueueProvider queueProvider, IEnumerable<string> queues)
	{
		if (queueProvider == null) throw new ArgumentNullException(nameof(queueProvider));
		if (queues == null) throw new ArgumentNullException(nameof(queues));

		providers.Add(queueProvider);

		foreach (string queue in queues)
		{
			if (providersByQueue.ContainsKey(queue)) throw new ArgumentException($"Queue [{queue}] already exists", nameof(queue));
			providersByQueue.Add(queue, queueProvider);
		}
	}

	public IPersistentJobQueueProvider GetProvider(string queue) => providersByQueue.ContainsKey(queue) ? providersByQueue[queue] : provider;
}