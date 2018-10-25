// ReSharper disable UseOfImplicitGlobalInFunctionScope

/**
 * Heartbeat Server
 * @param {string} id - the server id
 * @param {number} heartbeat = the epoch time
 */
function heartbeatServer(id: string, heartbeat: number) {
    let context: IContext = getContext();
    let collection: ICollection = context.getCollection();
    let response: IResponse = getContext().getResponse();

    // default response
    response.setBody(false);

    let filter = (doc: IServer) => doc.type === 1 && doc.server_id === id;

    let result: IQueryResponse = collection.filter(filter, (error: IRequestCallbackError, docs: Array<IServer>) => {
        if (error) {
            throw error;
        }
        if (docs.length === 0) {
            throw new Error(`No server found for id :${id}`);
        }
        if (docs.length > 1) {
            throw new Error(`Found more than one server for :${id}`);
        }

        let doc: IServer = docs.shift();
        doc.last_heartbeat = heartbeat;

        let isAccepted: boolean = collection.replaceDocument(doc._self, doc, (error: IRequestCallbackError) => {
            if (error) {
                throw error;
            }
            response.setBody(true);
        });

        if (!isAccepted) {
            throw new Error("The call was not accepted");
        }
    });

    if (!result.isAccepted) {
        throw new Error("The call was not accepted");
    }
}