interface IData<T> {
    items: Array<T>
}

interface IDocumentBase extends IDocumentMeta {
    type: number;
    expire_on: number;
}

interface IProcedureResponse {
    affected: number;
    continuation: boolean;
}

interface IServer extends IDocumentBase {
    server_id: string;
    workers: number;
    queues: Array<string>;
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
    counter_type: number;
}