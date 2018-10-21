interface IData<T> {
    items: Array<T>
}

interface ISet extends IDocumentMeta {
    key: string;
    value: string;
    score?: number;
    type: number;
}
