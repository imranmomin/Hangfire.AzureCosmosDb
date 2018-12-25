/**
 * Expire Documents
 * @param {string} query - the query to get only those documents which need to be expired
 */
function expireDocument(query: string, expireOn: number) {
    let context: IContext = getContext();
    let collection: ICollection = context.getCollection();
    let response: IResponse = getContext().getResponse();
    let collectionLink = collection.getSelfLink();
    let responseBody: IProcedureResponse = {
        affected: 0,
        continuation: false
    };

    if (query === undefined || query === null) {
        throw new Error("query is either empty or null");
    }

    // append the query to filter by expire_on is not defined or less than $expireOn
    query = `${query} AND (NOT IS_DEFINED(doc.expire_on) OR doc.expire_on !== ${expireOn})`;

    // default response
    response.setBody(responseBody);

    // Recursively runs the query w/ support for continuation tokens.
    // Calls tryUpdate(documents) as soon as the query returns documents.
    function tryQueryAndUpdate(continuation?: string) {
        let feedOptions: IFeedOptions = {
            continuation: continuation,
            pageSize: 10
        };

        let result: boolean = collection.queryDocuments(collectionLink, query, feedOptions, (error: IFeedCallbackError, docs: Array<IDocumentBase>, feedCallbackOptions: IFeedCallbackOptions) => {
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
        if (!result) {
            responseBody.continuation = true;
            response.setBody(responseBody);
        }
    }

    // Recursively update documents passed in as an array argument.
    // Attempts to query for more on empty array.
    function tryUpdate(documents: Array<IDocumentBase>) {
        if (documents.length > 0) {

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