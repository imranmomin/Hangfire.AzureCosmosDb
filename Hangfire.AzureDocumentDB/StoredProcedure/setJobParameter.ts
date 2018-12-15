/**
 * Set the parameter for the job
 * @param {string} id - the job id
 * @param {IParameter} parameter - the parameter object
 */
function setJobParameter(id: string, parameter: IParameter) {
    let context: IContext = getContext();
    let collection: ICollection = context.getCollection();
    let response: IResponse = getContext().getResponse();
    let collectionLink: string = collection.getAltLink();
    let documentLink: string = `${collectionLink}/docs/${id}/`;

    // default response
    response.setBody(false);

    let isAccepted: boolean = collection.readDocument(documentLink, (error: IRequestCallbackError, doc: IJob) => {
        if (error) {
            throw error;
        }

        if (doc.parameters === undefined || doc.parameters === null || doc.parameters.length === 0) {
            doc.parameters = new Array<IParameter>();

        } else {
            doc.parameters = doc.parameters.filter((p: IParameter) => p.name !== parameter.name);
        }

        // add the new parameter
        doc.parameters.push(parameter);
        let options: IReplaceOptions = { etag: doc._etag };

        let result: boolean = collection.replaceDocument(doc._self, doc, options, (err: IRequestCallbackError) => {
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