function persistJob(id) {
    let context = getContext();
    let collection = context.getCollection();
    let response = getContext().getResponse();
    let collectionLink = collection.getSelfLink();
    let documentLink = `${collectionLink}/docs/${id}`;
    response.setBody(false);
    let isAccepted = collection.readDocument(documentLink, (error, doc) => {
        if (error) {
            throw error;
        }
        if (doc.type !== 2) {
            throw new Error("The document is not of type `Job`");
        }
        if (doc.expire_on === undefined || doc.expire_on === null) {
            response.setBody(true);
            return;
        }
        delete doc.expire_on;
        let result = collection.replaceDocument(doc._self, doc, (err) => {
            if (err) {
                throw err;
            }
            response.setBody(true);
        });
        if (!result) {
            throw new Error("The call was not accepted");
        }
    });
    if (!isAccepted) {
        throw new Error("The call was not accepted");
    }
}
