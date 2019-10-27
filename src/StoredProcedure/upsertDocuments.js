function upsertDocuments(data) {
    let context = getContext();
    let collection = context.getCollection();
    let response = getContext().getResponse();
    let collectionLink = collection.getSelfLink();
    if (data.items.length === 0) {
        response.setBody(0);
        return;
    }
    let count = 0;
    let docs = data.items;
    let docsLength = docs.length;
    tryUpsert(docs[count]);
    function tryUpsert(doc) {
        let isAccepted = collection.upsertDocument(collectionLink, doc, callback);
        if (!isAccepted) {
            response.setBody(count);
        }
    }
    function callback(err) {
        if (err) {
            throw err;
        }
        count++;
        if (count >= docsLength) {
            response.setBody(count);
        }
        else {
            tryUpsert(docs[count]);
        }
    }
}
