function setJobState(id, state) {
    let context = getContext();
    let collection = context.getCollection();
    let response = getContext().getResponse();
    let collectionLink = collection.getAltLink();
    let documentLink = `${collectionLink}/docs/${id}`;
    response.setBody(false);
    let isAccepted = collection.readDocument(documentLink, (error, job) => {
        if (error) {
            throw error;
        }
        createState(state, (doc) => {
            job.state_id = doc.id;
            job.state_name = doc.name;
            let options = { etag: job._etag };
            let success = collection.replaceDocument(job._self, job, options, (err) => {
                if (err) {
                    throw err;
                }
                response.setBody(true);
            });
            if (!success) {
                throw new Error("The call was not accepted");
            }
        });
    });
    function createState(doc, callback) {
        let success = collection.createDocument(collectionLink, doc, (error, document) => {
            if (error) {
                throw error;
            }
            callback(document);
        });
        if (!success) {
            throw new Error("The call was not accepted");
        }
    }
    if (!isAccepted) {
        throw new Error("The call was not accepted");
    }
}
