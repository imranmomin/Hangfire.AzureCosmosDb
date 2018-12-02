/**
 * Announce server
 * @param {IServer} server - the server document
 */
function announceServer(server: IServer) {
    let context: IContext = getContext();
    let collection: ICollection = context.getCollection();
    let collectionLink: string = collection.getAltLink();
    let response: IResponse = getContext().getResponse();
    let documentLink: string = `${collectionLink}/docs/${server.id}`;

    // default response
    response.setBody(false);

    let result: boolean = collection.readDocument(documentLink, (error: IFeedCallbackError, doc: IServer) => {
        if (error !== undefined && error.number !== 404) {
            throw error;
        }

        if (doc === undefined) {
            doc = server;
        } else {
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

    if (!result) {
        throw new Error("The call was not accepted");
    }
}