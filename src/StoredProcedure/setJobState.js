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
        job.state_id = state.id;
        job.state_name = state.name;
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
    if (!isAccepted) {
        throw new Error("The call was not accepted");
    }
}
