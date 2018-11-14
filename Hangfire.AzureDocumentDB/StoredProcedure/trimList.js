function trimList(key, startIndex, endIndex) {
    let context = getContext();
    let collection = context.getCollection();
    let response = getContext().getResponse();
    let collectionLink = collection.getSelfLink();
    response.setBody(false);
    let query = `SELECT * FROM doc WHERE doc.type === 5 and doc.key === '${key}' ORDER BY doc.created_on DESC`;
    let result = collection.queryDocuments(collectionLink, query, (error, docs) => {
        if (error) {
            throw error;
        }
        for (let index = 0; index < docs.length; index++) {
            if (index < startIndex || index > endIndex) {
                const newLocal = docs[index];
                let isAccepted = collection.deleteDocument(newLocal._self, err => {
                    if (err)
                        throw err;
                });
                if (!isAccepted) {
                    throw new Error("Failed to remove keys");
                }
            }
        }
    });
    if (!result) {
        throw new Error("The call was not accepted");
    }
}
