/**
 * Expiration manager to delete old expired documents
 * @param {number} docType - The type of the document to delete
 * @param {number} expireOn - The unix timestamp to expire documents
 */
function deleteExpiredDocuments(docType, expireOn) {
    var context = getContext();
    var collection = context.getCollection();
    var response = context.getResponse();

    var result = collection.filter(function (doc) {
        return doc.type === docType && doc.expire_on <= expireOn;
    }, function (err, documents) {
        response.setBody(0);
        if (err) throw err;

        for (var index = 0; index < documents.length; index++) {
            var doc = documents[index];
            if (docType === 4 && doc.type === docType && doc.counter_type === 2) continue;
            collection.deleteDocument(doc._self);
        }

        response.setBody(documents.length);
    });

    if (!result.isAccepted) throw new ("The call was not accepted");
}