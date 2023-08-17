/**
 * Deletes all the document returned back from the query
 * @param {string} query - the query to get documents
 */
function deleteDocuments(query: string) {
    let context: IContext = getContext();
    let collection: ICollection = context.getCollection();
    let collectionLink: string = collection.getSelfLink();
    let response: IResponse = getContext().getResponse();
    let responseBody: IProcedureResponse = {
        affected: 0,
        continuation: false
    };

    // default response
    response.setBody(responseBody);

    tryQueryAndDelete();

    // Recursively runs the query w/ support for continuation tokens.
    // Calls tryDelete(documents) as soon as the query returns documents.
    function tryQueryAndDelete(continuation?: string) {
        let feedOptions: IFeedOptions = {
            continuation: continuation,
            pageSize: 10
        };

        let result: boolean = collection.queryDocuments(collectionLink, query, feedOptions, (error: IFeedCallbackError, docs: Array<IDocumentBase>, feedCallbackOptions: IFeedCallbackOptions) => {
            if (error) {
                throw error;
            }

            if (docs.length > 0) {
                // Begin deleting documents as soon as documents are returned form the query results.
                // tryDelete() resumes querying after deleting; no need to page through continuation tokens.
                //  - this is to prioritize writes over reads given timeout constraints.
                tryDelete(docs);
            } else if (feedCallbackOptions.continuation) {
                // Else if the query came back empty, but with a continuation token; repeat the query w/ the token.
                tryQueryAndDelete(feedCallbackOptions.continuation);
            } else {
                // Else if there are no more documents and no continuation token - we are finished deleting documents.
                responseBody.continuation = false;
                response.setBody(responseBody);
            }
        });

        // If we hit execution bounds - return continuation: true.
        if (!result) {
            responseBody.continuation = true;
            response.setBody(responseBody);
        }
    }

    // Recursively deletes documents passed in as an array argument.
    // Attempts to query for more on empty array.
    function tryDelete(documents: Array<IDocumentBase>) {
        if (documents.length > 0) {
            // Delete the first document in the array.
            let isAccepted: boolean = collection.deleteDocument(documents[0]._self, (error: IRequestCallbackError) => {
                if (error) {
                    throw error;
                }

                responseBody.affected++;
                documents.shift();
                // Delete the next document in the array.
                tryDelete(documents);
            });

            // If we hit execution bounds - return continuation: true.
            if (!isAccepted) {
                response.setBody(responseBody);
            }
        } else {
            // If the document array is empty, query for more documents.
            tryQueryAndDelete();
        }
    }
}