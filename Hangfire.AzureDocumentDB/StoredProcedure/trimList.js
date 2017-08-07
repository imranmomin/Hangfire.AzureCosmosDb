/**
 * Removes item from list not within the range
 * @param {string} key - the key for the set
 * @param {number} startIndex - the start index
 * * @param {number} endIndex - the end index
 * @returns {boolean} true if success; else false 
 */
function removeFromList(key, startIndex, endIndex) {
    var context = getContext();
    var collection = context.getCollection();
    var response = context.getResponse();

    var result = collection.filter(function (doc) {
        return doc.type === 5 && doc.key === key;
    }, function (err, documents) {
        response.setBody(false);
        if (err) throw err;

        if (documents.length > 0 && documents.length < endIndex) throw new ("End index is more then the length of the document.");

        for (var index = 0; index < documents.length; index++) {
            if (index < startIndex || index > endIndex) {
                var self = documents[index]._self;
                collection.deleteDocument(self);
            }
        }

        response.setBody(true);
    });

    if (!result.isAccepted) throw new ("The call was not accepted");
}