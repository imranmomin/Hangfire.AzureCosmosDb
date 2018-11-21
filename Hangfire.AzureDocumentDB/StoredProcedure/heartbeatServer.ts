// ReSharper disable UseOfImplicitGlobalInFunctionScope

/**
 * Heartbeat Server
 * @param {string} id - the server id
 * @param {number} heartbeat = the epoch time
 */
function heartbeatServer(id: string, heartbeat: number) {
    let context: IContext = getContext();
    let collection: ICollection = context.getCollection();
    let collectionLink: string = collection.getSelfLink();
    let response: IResponse = getContext().getResponse();
    let documentLink: string = `${collectionLink}/docs/${id}`;

    // default response
    response.setBody(false);
    
    let result: boolean = collection.readDocument(documentLink, (error: IRequestCallbackError, doc: IServer) => {
        if (error) {
            throw error;
        }

        if (doc === undefined) {
            response.setBody(false);
            return;
        }
       
        // set the heartbeat 
        doc.last_heartbeat = heartbeat;

        let isAccepted: boolean = collection.replaceDocument(doc._self, doc, (error: IRequestCallbackError) => {
            if (error) {
                throw error;
            }
            response.setBody(true);
        });

        if (!isAccepted) {
            throw new Error("The call was not accepted");
        }
    });

    if (!result) {
        throw new Error("The call was not accepted");
    }
}