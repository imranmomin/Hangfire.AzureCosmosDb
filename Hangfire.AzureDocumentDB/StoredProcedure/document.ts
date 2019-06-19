interface IData<T> {
	items: Array<T>
}

interface IDocumentBase extends IDocumentMeta {
	type: number;
	// ReSharper disable once InconsistentNaming
	expire_on: number;
}

interface IProcedureResponse {
	affected: number;
	continuation: boolean;
}

interface IServer extends IDocumentBase {
	// ReSharper disable once InconsistentNaming
	server_id: string;
	workers: number;
	queues: Array<string>;
	// ReSharper disable once InconsistentNaming
	last_heartbeat: number;
}

interface ISet extends IDocumentBase {
	key: string;
	value: string;
	score?: number;
}

interface ICounter extends IDocumentBase {
	key: string;
	value: number;
	// ReSharper disable once InconsistentNaming
	counter_type: number;
}

interface IList extends IDocumentBase {
	key: string;
	value: string;
}

interface IHash extends IDocumentBase {
	key: string;
	field: string;
	value: string;
}

interface IJob extends IDocumentBase {
	arguments: string;
	// ReSharper disable once InconsistentNaming
	state_id: string;
	// ReSharper disable once InconsistentNaming
	state_name: string;
	parameters: Array<IParameter>;
}

interface IParameter {
	name: string;
	value: string;
}

interface IState extends IDocumentBase {
	// ReSharper disable once InconsistentNaming
	job_id: string;
	name: string;
	reason: string;
	data: { [key: string]: string };
}