/**
 * Removes item from set releated to key/value
 * @param {string} key - the key for the set
 * @param {Object} value - the value for the key
 * @returns {boolean} true if success; else false 
 */
function removeFromSet(key, value) {
    var context = getContext();
    var collection = context.getCollection();
    var response = context.getResponse();

    var result = collection.filter(function (doc) {
        return doc.type === 7 && doc.key === key && doc.value === value;
    }, function (err, documents) {
        if (err) throw err;
        response.setBody(false);

        if (documents.length === 0) throw new ("No item found for key :" + key);
        if (documents.length > 1) throw new ("Found more than item found for key :" + key);

        var self = documents[0]._self;
        collection.deleteDocument(self);

        response.setBody(true);
    });

    if (!result.isAccepted) throw new ("The call was not accepted");
}