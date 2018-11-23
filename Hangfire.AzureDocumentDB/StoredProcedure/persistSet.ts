/**
 * Persist Set
 * @param {string} key - the set key
 */
function persistSet(key: string) {
    let context: IContext = getContext();
    let collection: ICollection = context.getCollection();
    let response: IResponse = getContext().getResponse();
    let responseBody: IProcedureResponse = {
        affected: 0,
        continuation: true
    };

    // default response
    response.setBody(responseBody);

    // filter
    let filter = (doc: ISet) => doc.type === 7 && doc.key === key &&  doc.expire_on !== undefined;

    // Recursively runs the query w/ support for continuation tokens.
    // Calls tryUpdate(documents) as soon as the query returns documents.
    function tryQueryAndUpdate(continuation?: string) {
        let feedOptions: IFeedOptions = {
            continuation: continuation
        };

        let result: IQueryResponse = collection.filter(filter, feedOptions, (error: IFeedCallbackError, docs: Array<IDocumentBase>, feedCallbackOptions: IFeedCallbackOptions) => {
            if (error) {
                throw error;
            }

            if (docs.length > 0) {
                // Begin replacing documents as soon as documents are returned form the query results.
                // tryUpdate() resumes querying after replacing; no need to page through continuation tokens.
                //  - this is to prioritize writes over reads given timeout constraints.
                tryUpdate(docs);
            } else if (feedCallbackOptions.continuation) {
                // Else if the query came back empty, but with a continuation token; repeat the query w/ the token.
                tryQueryAndUpdate(feedCallbackOptions.continuation);
            } else {
                // Else if there are no more documents and no continuation token - we are finished deleting documents.
                responseBody.continuation = false;
                response.setBody(responseBody);
            }
        });

        // If we hit execution bounds - return continuation: true.
        if (!result.isAccepted) {
            response.setBody(responseBody);
        }
    }

    // Recursively update documents passed in as an array argument.
    // Attempts to query for more on empty array.
    function tryUpdate(documents: Array<IDocumentBase>) {
        if (documents.length > 0) {
            // replace the first document in the array.

            let doc: IDocumentBase = documents[0];
            doc.expire_on = expireOn;

            let option: IReplaceOptions = {
                etag: doc._etag
            }

            let isAccepted: boolean = collection.replaceDocument(doc._self, doc, option, (error: IRequestCallbackError) => {
                if (error) {
                    throw error;
                }

                responseBody.affected++;
                documents.shift();

                // update the next document in the array.
                tryUpdate(documents);
            });

            // If we hit execution bounds - return continuation: true.
            if (!isAccepted) {
                response.setBody(responseBody);
            }
        } else {
            // If the document array is empty, query for more documents.
            tryQueryAndUpdate();
        }
    }
}