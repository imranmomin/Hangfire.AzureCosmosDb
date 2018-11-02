/**
 * Remove key from Hash
 * @param {string} key - the key for the set
 * @param {IData<IHash>} data - the array of hash
 */
function setRangeHash(key: string, data: IData<IHash>) {
    let context: IContext = getContext();
    let collection: ICollection = context.getCollection();
    let response: IResponse = getContext().getResponse();
    let collectionLink: string = collection.getSelfLink();

    if (data.items.length === 0) {
        response.setBody(0);
        return;
    }

    let fitler = (doc: IHash) => doc.type === 6 && doc.key === key;

    let result: IQueryResponse = collection.filter(fitler, (error: IRequestCallbackError, docs: Array<IHash>) => {
        if (error) {
            throw error;
        }

        let hashes: Array<IHash> = new Array<IHash>();

        for (let index: number = 0; index < data.items.length; index++) {
            let soruce: IHash = data.items[index];
            let hash: IHash = docs.find((h: IHash) => h.field === soruce.field);

            if (hash) {
                hash.value == soruce.value;
            } else {
                hash = soruce;
            }

            hashes.push(hash);
        }
        
        let count: number = 0;
        let docsLength: number = hashes.length;

        // Call the CRUD API to upsert a document.
        tryUpsert(hashes[count]);

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
            if (err) {
                throw err;
            }

            // One more document has been inserted/updated, increment the count.
            count++;

            if (count >= docsLength) {
                // If we have inserted/updated all documents, we are done. Just set the response.
                response.setBody(count);
            } else {
                // upsert next document.
                tryUpsert(hashes[count]);
            }
        }
    });

    if (!result.isAccepted) {
        throw new Error("The call was not accepted");
    }
}