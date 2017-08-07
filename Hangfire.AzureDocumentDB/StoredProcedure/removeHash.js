/**
 * Remove key from Hash
 * @param {string} key - the key for the set
 * @returns {boolean} true if success; else false 
 */
function removeHash(key) {
    var context = getContext();
    var collection = context.getCollection();
    var response = context.getResponse();

    var result = collection.filter(function (doc) {
        return doc.type === 6 && doc.key === key;
    }, function (err, documents) {
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