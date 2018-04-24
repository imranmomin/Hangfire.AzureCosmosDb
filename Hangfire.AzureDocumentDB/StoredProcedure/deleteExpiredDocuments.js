// ReSharper disable UseOfImplicitGlobalInFunctionScope

/**
 * Expiration manager to delete old expired documents
 * @param {number} docType - The type of the document to delete
 * @param {number} expireOn - The unix timestamp to expire documents
 */
function deleteExpiredDocuments(docType, expireOn) {
    var result = __.filter(function (doc) {
        return doc.type === docType && doc.expire_on <= expireOn;
    }, function (err, docs) {
        if (err) throw err;

        var deleted = 0;
        for (var index = 0; index < docs.length; index++) {
            var doc = docs[index];
            if (docType === 4 && doc.type === docType && doc.counter_type === 2) continue;
            var isAccepted = __.deleteDocument(doc._self, function (error) {
                if (error) throw error;
            });

            if (!isAccepted) throw new Error("Failed to delete expired documents");
            else deleted += 1;
        }

        getContext().getResponse().setBody(deleted);
    });

    if (!result.isAccepted) throw new Error("The call was not accepted");
}