function deleteExpiredDocuments(docType, expireOn) {
    let context = getContext();
    let collection = context.getCollection();
    let response = getContext().getResponse();
    let responseBody = {
        affected: 0,
        continuation: true
    };
    let filter = (doc) => {
        if (doc.type === docType && doc.expire_on <= expireOn) {
            if (docType === 4) {
                let counter = doc;
                if (counter.counter_type === 1) {
                    return false;
                }
            }
            return true;
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
            let isAccepted = collection.deleteDocument(documents[0]._self, {}, (error) => {
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
