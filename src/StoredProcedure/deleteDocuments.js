function deleteDocuments(query) {
    let context = getContext();
    let collection = context.getCollection();
    let collectionLink = collection.getSelfLink();
    let response = getContext().getResponse();
    let responseBody = {
        affected: 0,
        continuation: false
    };
    response.setBody(responseBody);
    tryQueryAndDelete();
    function tryQueryAndDelete(continuation) {
        let feedOptions = {
            continuation: continuation,
            pageSize: 10
        };
        let result = collection.queryDocuments(collectionLink, query, feedOptions, (error, docs, feedCallbackOptions) => {
            if (error) {
                throw error;
            }
            if (docs.length > 0) {
                tryDelete(docs);
            }
            else if (feedCallbackOptions.continuation) {
                tryQueryAndDelete(feedCallbackOptions.continuation);
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
    function tryDelete(documents) {
        if (documents.length > 0) {
            let isAccepted = collection.deleteDocument(documents[0]._self, (error) => {
                if (error) {
                    throw error;
                }
                responseBody.affected++;
                documents.shift();
                tryDelete(documents);
            });
            if (!isAccepted) {
                response.setBody(responseBody);
            }
        }
        else {
            tryQueryAndDelete();
        }
    }
}
