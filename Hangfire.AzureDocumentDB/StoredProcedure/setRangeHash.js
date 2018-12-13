function setRangeHash(data) {
    let context = getContext();
    let collection = context.getCollection();
    let response = getContext().getResponse();
    let collectionLink = collection.getSelfLink();
    if (data.items.length === 0) {
        response.setBody(0);
        return;
    }
    let count = 0;
    let hashes = data.items;
    let docsLength = hashes.length;
    tryUpsert(hashes[count]);
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
            tryUpsert(hashes[count]);
        }
    }
}
