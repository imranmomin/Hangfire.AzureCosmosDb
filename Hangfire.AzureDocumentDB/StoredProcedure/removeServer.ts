/**
 * Remove Server
 * @param {string} id - the server id
 */
function removeServer(id: string) {
    let context: IContext = getContext();
    let collection: ICollection = context.getCollection();
    let response: IResponse = getContext().getResponse();

    // default response
    response.setBody(false);

    let filter = (doc: IServer) => doc.type === 1 && doc.server_id === id;

    let result: IQueryResponse = collection.filter(filter, (error: IRequestCallbackError, docs: Array<IServer>) => {
        if (error) {
            throw error;
        }

        if (docs.length === 0) {
            throw new Error(`No server found for id :${id}`);
        }

        if (docs.length > 1) {
            throw new Error(`Found more than one server for :${id}`);
        }

        let doc: IServer = docs.shift();
        let isAccepted: boolean = collection.deleteDocument(doc._self, (err: IRequestCallbackError) => {
            if (err) {
                throw err;
            }
            response.setBody(true);
        });

        if (!isAccepted) {
            throw new Error("The call was not accepted");
        }
    });

    if (!result.isAccepted) {
        throw new Error("The call was not accepted");
    }
}