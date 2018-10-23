function addToSet(set) {
    let context = getContext();
    let collection = context.getCollection();
    let collectionLink = collection.getSelfLink();
    let response = getContext().getResponse();
    let filter = (doc) => doc.type === set.type && doc.key === set.key;
    let result = collection.filter(filter, (error, docs) => {
        if (error)
            throw error;
        docs = docs.filter((doc) => doc.value === set.value);
        if (docs.length > 0) {
            docs.forEach((doc) => doc.score = set.score);
        }
        else {
            docs = new Array();
            docs.push(set);
        }
        let count = 0;
        let docsLength = docs.length;
        tryUpsert(docs[count]);
        function tryUpsert(doc) {
            let isAccepted = collection.upsertDocument(collectionLink, doc, callback);
            if (!isAccepted) {
                response.setBody(count);
            }
        }
        function callback(err) {
            if (err)
                throw err;
            count++;
            if (count >= docsLength) {
                response.setBody(count);
            }
            else {
                tryUpsert(docs[count]);
            }
        }
    });
    if (!result.isAccepted) {
        throw new Error("The call was not accepted");
    }
}
