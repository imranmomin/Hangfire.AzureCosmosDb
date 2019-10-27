/**
 * Upsert documents
 * @param {IData<IDocumentBase>} data - the array of documents
 */
function upsertDocuments(data: IData<IDocumentBase>) {
	let context: IContext = getContext();
	let collection: ICollection = context.getCollection();
	let response: IResponse = getContext().getResponse();
	let collectionLink: string = collection.getSelfLink();

	if (data.items.length === 0) {
		response.setBody(0);
		return;
	}

	let count: number = 0;
	let docs: Array<IDocumentBase> = data.items;
	let docsLength: number = docs.length;

	// Call the CRUD API to upsert a document.
	tryUpsert(docs[count]);

	// Note that there are 2 exit conditions:
	// 1) The upsertDocument request was not accepted. In this case the callback will not be called, we just call setBody and we are done.
	// 2) The callback was called docs.length times. In this case all documents were created/updated and we don't need to call tryUpsert anymore. 
	//    Just call setBody and we are done.
	function tryUpsert(doc: IDocumentBase) {
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
			tryUpsert(docs[count]);
		}
	}
}