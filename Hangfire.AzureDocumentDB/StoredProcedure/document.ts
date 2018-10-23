interface IData<T> {
    items: Array<T>
}

interface IServer {
    server_id: string;
    workers: number;
    queues: Array<string>;
    last_heartbeat: number;
    type: number;
}

interface ISet extends IDocumentMeta {
    key: string;
    value: string;
    score?: number;
    type: number;
}
