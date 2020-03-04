/**
 * Set the Job state data
 * @param {string} id - the job id
 * @param {IState} state - the state document
 */
function setJobState(id: string, state: IState) {
    let context: IContext = getContext();
    let collection: ICollection = context.getCollection();
    let response: IResponse = getContext().getResponse();
    let collectionLink: string = collection.getAltLink();
    let documentLink: string = `${collectionLink}/docs/${id}`;

    // default response
    response.setBody(false);

    let isAccepted: boolean = collection.readDocument(documentLink, (error: IRequestCallbackError, job: IJob) => {
        if (error) {
            throw error;
        }
               
        job.state_id = state.id;
        job.state_name = state.name;
        let options: IReplaceOptions = { etag: job._etag };

        let success: boolean = collection.replaceDocument(job._self, job, options, (err: IRequestCallbackError) => {
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