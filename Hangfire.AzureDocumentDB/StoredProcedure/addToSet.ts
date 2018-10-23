/**
 * Upsert a set document to the collection
 * @param {ISet} set - the set document
 */
function addToSet(set: ISet) {
    let context: IContext = getContext();
    let collection: ICollection = context.getCollection();
    let collectionLink: string = collection.getSelfLink();
    let response: IResponse = getContext().getResponse();

    // filter function to find the duplicate sets
    let filter = (doc: ISet) => doc.type === set.type && doc.key === set.key;

    let result: IQueryResponse = collection.filter(filter, (error: IFeedCallbackError, docs: Array<ISet>) => {
        if (error) throw error;

        // filter only those docs where the value matches
        docs = docs.filter((doc: ISet) => doc.value === set.value);

        // if any - then it is an update
        if (docs.length > 0) {
            docs.forEach((doc: ISet) => doc.score = set.score);
        } else {
            docs = new Array<ISet>();
            docs.push(set);
        }

        let count: number = 0;
        let docsLength: number = docs.length;

        // Call the CRUD API to upsert a document.
        tryUpsert(docs[count]);

        // Note that there are 2 exit conditions:
        // 1) The upsertDocument request was not accepted. In this case the callback will not be called, we just call setBody and we are done.
        // 2) The callback was called docs.length times. In this case all documents were created/updated and we don't need to call tryUpsert anymore. 
        //    Just call setBody and we are done.
        function tryUpsert(doc: ISet) {
            let isAccepted: boolean = collection.upsertDocument(collectionLink, doc, callback);

            // If the request was accepted, callback will be called.
            // Otherwise report current count back to the client, 
            // which will call the script again with remaining set of docs.
            // This condition will happen when this stored procedure has been running too long
            // and is about to get cancelled by the server. This will allow the calling client
            // to resume this batch from the point we got to before isAccepted was set to false
            if (!isAccepted) {
                response.setBody(count);
            }
        }

        // This is called when collection.upsertDocument is done and the document has been persisted.
        function callback(err: IRequestCallbackError) {
            if (err) throw err;

            // One more document has been inserted/updated, increment the count.
            count++;

            if (count >= docsLength) {
                // If we have inserted/updated all documents, we are done. Just set the response.
                response.setBody(count);
            } else {
                // upsert next document.
                tryUpsert(docs[count]);
            }
        }
    });

    if (!result.isAccepted) {
        throw new Error("The call was not accepted");
    }
}