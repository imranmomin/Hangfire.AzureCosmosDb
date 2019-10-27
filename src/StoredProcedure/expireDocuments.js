function expireDocument(query, expireOn) {
    let context = getContext();
    let collection = context.getCollection();
    let response = getContext().getResponse();
    let collectionLink = collection.getSelfLink();
    let responseBody = {
        affected: 0,
        continuation: false
    };
    if (query === undefined || query === null) {
        throw new Error("query is either empty or null");
    }
    query = `${query} AND (NOT IS_DEFINED(doc.expire_on) OR doc.expire_on !== ${expireOn})`;
    response.setBody(responseBody);
    function tryQueryAndUpdate(continuation) {
        let feedOptions = {
            continuation: continuation,
            pageSize: 10
        };
        let result = collection.queryDocuments(collectionLink, query, feedOptions, (error, docs, feedCallbackOptions) => {
            if (error) {
                throw error;
            }
            if (docs.length > 0) {
                tryUpdate(docs);
            }
            else if (feedCallbackOptions.continuation) {
                tryQueryAndUpdate(feedCallbackOptions.continuation);
            }
            else {
                responseBody.continuation = false;
                response.setBody(responseBody);
            }
        });
        if (!result) {
            responseBody.continuation = true;
            response.setBody(responseBody);
        }
    }
    function tryUpdate(documents) {
        if (documents.length > 0) {
            let doc = documents[0];
            doc.expire_on = expireOn;
            let option = {
                etag: doc._etag
            };
            let isAccepted = collection.replaceDocument(doc._self, doc, option, (error) => {
                if (error) {
                    throw error;
                }
                responseBody.affected++;
                documents.shift();
                tryUpdate(documents);
            });
            if (!isAccepted) {
                response.setBody(responseBody);
            }
        }
        else {
            tryQueryAndUpdate();
        }
    }
}
