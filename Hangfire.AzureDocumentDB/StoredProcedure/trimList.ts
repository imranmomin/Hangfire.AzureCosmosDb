/**
 * Removes item from list not within the range
 * @param {string} key - the key for the set
 * @param {number} startIndex - the start index
 * @param {number} endIndex - the end index
 */
function trimList(key: string, startIndex: number, endIndex: number) {
    let context: IContext = getContext();
    let collection: ICollection = context.getCollection();
    let response: IResponse = getContext().getResponse();
    let collectionLink: string = collection.getSelfLink();

    // default response
    response.setBody(false);

    let query: string = `SELECT * FROM doc WHERE doc.type === 5 and doc.key === '${key}' ORDER BY doc.created_on DESC`;

    let result: boolean = collection.queryDocuments(collectionLink, query, (error: IFeedCallbackError, docs: Array<IList>) => {
        if (error) {
            throw error;
        }

        for (let index: number = 0; index < docs.length; index++) {
            if (index < startIndex || index > endIndex) {
                const newLocal: IList = docs[index];
                let isAccepted = collection.deleteDocument(newLocal._self, err => {
                    if (err) throw err;
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