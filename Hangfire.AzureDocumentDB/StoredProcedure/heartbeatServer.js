function heartbeatServer(id, heartbeat) {
    let context = getContext();
    let collection = context.getCollection();
    let response = getContext().getResponse();
    response.setBody(false);
    let filter = (doc) => doc.type === 1 && doc.server_id === id;
    let result = collection.filter(filter, (error, docs) => {
        if (error) {
            throw error;
        }
        if (docs.length === 0) {
            throw new Error(`No server found for id :${id}`);
        }
        if (docs.length > 1) {
            throw new Error(`Found more than one server for :${id}`);
        }
        let doc = docs.shift();
        doc.last_heartbeat = heartbeat;
        let isAccepted = collection.replaceDocument(doc._self, doc, (error) => {
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
