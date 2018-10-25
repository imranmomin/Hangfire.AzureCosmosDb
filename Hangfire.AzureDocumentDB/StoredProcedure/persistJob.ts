/**
 * Expires a job
 * @param {string} id - the job id
 */
function persistJob(id: string) {
    let context: IContext = getContext();
    let collection: ICollection = context.getCollection();
    let response: IResponse = getContext().getResponse();
    let collectionLink: string = collection.getSelfLink();
    let documentLink: string = `${collectionLink}/docs/${id}`;

    // default response
    response.setBody(false);

    let isAccepted: boolean = collection.readDocument(documentLink, (error: IRequestCallbackError, doc: IDocumentBase) => {
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

        // remove the expire_on property
        delete doc.expire_on;

        let result: boolean = collection.replaceDocument(doc._self, doc, (err: IRequestCallbackError) => {
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