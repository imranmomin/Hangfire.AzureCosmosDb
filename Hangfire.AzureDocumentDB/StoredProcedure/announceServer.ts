/**
 * Announce server
 * @param {IServer} server - the server document
 */
function announceServer(server: IServer) {
    let context: IContext = getContext();
    let collection: ICollection = context.getCollection();
    let collectionLink: string = collection.getSelfLink();
    let response: IResponse = getContext().getResponse();

    // filter function to find the duplicate servers
    let filter = (doc: IServer) => doc.type === server.type && doc.server_id === server.server_id;

    let result: IQueryResponse = collection.filter(filter, (error: IFeedCallbackError, docs: Array<IServer>) => {
        if (error) {
             throw error;
        }
        if (docs.length > 1) {
            throw new Error(`Found more than one server for: ${server.server_id}`);
        }

        let doc: IServer;
        if (docs.length === 0) {
            doc = server;
        } else {
            doc = docs.shift();
            doc.last_heartbeat = server.last_heartbeat;
            doc.workers = server.workers;
            doc.queues = server.queues;
        }

        let isAccepted: boolean = collection.upsertDocument(collectionLink, doc, (err: IRequestCallbackError) => {
            if (err) {
                throw err;
            }
            response.setBody(true);
        });

        if (!isAccepted) {
            throw new Error("Failed to create a server document");
        }
    });

    if (!result.isAccepted) {
        response.setBody(false);
        throw new Error("The call was not accepted");
    }
}