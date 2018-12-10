function announceServer(server) {
    let context = getContext();
    let collection = context.getCollection();
    let collectionLink = collection.getAltLink();
    let response = getContext().getResponse();
    let documentLink = `${collectionLink}/docs/${server.id}`;
    response.setBody(false);
    let result = collection.readDocument(documentLink, (error, doc) => {
        if (error !== undefined && error.number !== 404) {
            throw error;
        }
        if (doc === undefined || doc === null) {
            doc = server;
        }
        else {
            doc.last_heartbeat = server.last_heartbeat;
            doc.workers = server.workers;
            doc.queues = server.queues;
        }
        let isAccepted = collection.upsertDocument(collectionLink, doc, (err) => {
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
