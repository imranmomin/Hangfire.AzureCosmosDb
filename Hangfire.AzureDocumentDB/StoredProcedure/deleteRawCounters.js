function deleteRawCounters(ids) {
    let context = getContext();
    let collection = context.getCollection();
    let response = getContext().getResponse();
    let responseBody = {
        affected: 0,
        continuation: true
    };
    response.setBody(responseBody);
    let filter = (doc) => {
        if (doc.type === 4 && ids.items.length > 0) {
            let id = ids.items.find(d => d === doc.id);
            return id !== null;
        }
        return false;
    };
    tryQueryAndDelete();
    function tryQueryAndDelete(continuation) {
        let feedOptions = {
            continuation: continuation
        };
        let result = collection.filter(filter, feedOptions, (error, docs, feedCallbackOptions) => {
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
        if (!result.isAccepted) {
            response.setBody(responseBody);
        }
    }
    function tryDelete(documents) {
        if (documents.length > 0) {
            let doc = documents[0];
            let isAccepted = collection.deleteDocument(doc._self, (error) => {
                if (error) {
                    throw error;
                }
                responseBody.affected++;
                ids.items = ids.items.filter(d => d !== doc.id);
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
