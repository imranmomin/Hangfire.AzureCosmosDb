/**
 * Deletes the document if exists
 * @param {string} ids - the document ids
 * @returns {boolean} true if deleted; else false 
 */
function deleteDocumentIfExists(ids) {
    var context = getContext();
    var collection = context.getCollection();
    var response = context.getResponse();

    if (!Array.isArray(ids)) {
        ids = [ids];
    }

    var result = collection.filter(function (doc) { return ids.indexOf(doc.id) > -1; }, function (err, documents) {
        response.setBody(false);
        if (err) throw err;

        for (var index = 0; index < documents.length; index++) {
            var self = documents[index]._self;
            collection.deleteDocument(self);
        }

        response.setBody(true);
    });

    if (!result.isAccepted) throw new ("The call was not accepted");
}