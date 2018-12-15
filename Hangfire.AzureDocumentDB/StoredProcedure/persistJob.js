function persistJob(id) {
    let context = getContext();
    let collection = context.getCollection();
    let response = getContext().getResponse();
    let collectionLink = collection.getAltLink();
    let documentLink = `${collectionLink}/docs/${id}`;
    response.setBody(false);
    let isAccepted = collection.readDocument(documentLink, (error, doc) => {
        if (error) {
            throw error;
        }
        if (doc.expire_on === undefined || doc.expire_on === null) {
            response.setBody(true);
            return;
        }
        delete doc.expire_on;
        let options = { etag: doc._etag };
        let result = collection.replaceDocument(doc._self, doc, options, (err) => {
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
