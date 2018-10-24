/**
 * Sets the expire_on for a job
 * @param {string} id - the job id
 * @param {int} expireOn - the unix time when the job expires
 */
function expireJob(id: string, expireOn: number) {
    let context: IContext = getContext();
    let collection: ICollection = context.getCollection();
    let response: IResponse = getContext().getResponse();
    let collectionLink: string = collection.getSelfLink();
    let documentLink: string = `${collectionLink}/docs/${id}`;

    let isAccepted: boolean = collection.readDocument(documentLink, (error: IRequestCallbackError, doc: IDocumentBase) => {
        if (error) {
            throw error;
        }

        if (doc.type !== 2) {
            throw new Error("The document is not of type `Job`");
        }

        // set the expire_on 
        doc.expire_on = expireOn;

        let result: boolean = collection.replaceDocument(doc._self, doc, (err: IRequestCallbackError) => {
            if (err) {
                throw err;
            }
            response.setBody(true);
        });

        if (!result) throw new Error("The call was not accepted");

    });

    if (!isAccepted) throw new Error("The call was not accepted");
}