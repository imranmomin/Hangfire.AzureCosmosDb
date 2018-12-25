function setJobParameter(id, parameter) {
    let context = getContext();
    let collection = context.getCollection();
    let response = getContext().getResponse();
    let collectionLink = collection.getAltLink();
    let documentLink = `${collectionLink}/docs/${id}/`;
    response.setBody(false);
    let isAccepted = collection.readDocument(documentLink, (error, doc) => {
        if (error) {
            throw error;
        }
        if (doc.parameters === undefined || doc.parameters === null || doc.parameters.length === 0) {
            doc.parameters = new Array();
        }
        else {
            doc.parameters = doc.parameters.filter((p) => p.name !== parameter.name);
        }
        doc.parameters.push(parameter);
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
