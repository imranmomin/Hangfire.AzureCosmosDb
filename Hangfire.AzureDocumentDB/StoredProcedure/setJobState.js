function setJobState(id, state) {
    let context = getContext();
    let collection = context.getCollection();
    let response = getContext().getResponse();
    let collectionLink = collection.getSelfLink();
    let documentLink = `${collectionLink}/docs/${id}`;
    response.setBody(false);
    let isAccepted = collection.readDocument(documentLink, (error, job) => {
        if (error) {
            throw error;
        }
        if (job.type !== 2) {
            throw new Error("The document is not of type `Job`");
        }
        createState(state, (doc) => {
            job.state_id = doc.id;
            job.state_name = doc.name;
            let success = collection.replaceDocument(job._self, job, (err) => {
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
