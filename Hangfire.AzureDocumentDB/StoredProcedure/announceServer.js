function announceServer(server) {
    let context = getContext();
    let collection = context.getCollection();
    let collectionLink = collection.getSelfLink();
    let response = getContext().getResponse();
    let filter = (doc) => doc.type === server.type && doc.server_id === server.server_id;
    let result = collection.filter(filter, (error, docs) => {
        if (error) {
            throw error;
        }
        if (docs.length > 1) {
            throw new Error(`Found more than one server for: ${server.server_id}`);
        }
        let doc;
        if (docs.length === 0) {
            doc = server;
        }
        else {
            doc = docs.shift();
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
    if (!result.isAccepted) {
        response.setBody(false);
        throw new Error("The call was not accepted");
    }
}
