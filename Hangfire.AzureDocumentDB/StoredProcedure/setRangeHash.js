function setRangeHash(key, data) {
    let context = getContext();
    let collection = context.getCollection();
    let response = getContext().getResponse();
    let collectionLink = collection.getSelfLink();
    if (data.items.length === 0) {
        response.setBody(0);
        return;
    }
    let fitler = (doc) => doc.type === 6 && doc.key === key;
    let result = collection.filter(fitler, (error, docs) => {
        if (error) {
            throw error;
        }
        let hashes = new Array();
        for (let index = 0; index < data.items.length; index++) {
            let soruce = data.items[index];
            let hash = docs.find((h) => h.field === soruce.field);
            if (hash) {
                hash.value == soruce.value;
            }
            else {
                hash = soruce;
            }
            hashes.push(hash);
        }
        let count = 0;
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
    });
    if (!result.isAccepted) {
        throw new Error("The call was not accepted");
    }
}
