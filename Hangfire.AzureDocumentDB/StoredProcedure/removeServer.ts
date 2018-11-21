/**
 * Remove Server
 * @param {string} id - the server id
 */
function removeServer(id: string) {
    let context: IContext = getContext();
    let collection: ICollection = context.getCollection();
    let collectionLink: string = collection.getSelfLink();
    let response: IResponse = getContext().getResponse();
    let documentLink: string = `${collectionLink}/docs/${id}`;

    // default response
    response.setBody(false);

    let result: boolean = collection.deleteDocument(documentLink, (error: IRequestCallbackError) => {
        if (error) {
            throw error;
        }
        response.setBody(true);
    });

    if (!result) {
        throw new Error("The call was not accepted");
    }
}