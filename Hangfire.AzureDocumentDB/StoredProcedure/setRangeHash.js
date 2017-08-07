/**
 * Remove key from Hash
 * @param {string} key - the key for the set
 * @param {Array<Object>} sources - the array of hash
 * @returns {boolean} true if success; else false 
 */
function setRangeHash(key, sources) {
    var context = getContext();
    var collection = context.getCollection();
    var response = context.getResponse();

    var result = collection.filter(function (doc) {
        return doc.type === 6 && doc.key === key;
    }, function (err, documents) {
        response.setBody(0);
        if (err) throw err;

        var hashes = [];
        for (var index = 0; index < sources.length; index++) {
            var hash = match(documents, sources[index]);
            hashes.push(hash);
        }

        // upsert all the hash documents
        for (var j = 0; j < hashes.length; j++) {
            var hashDocument = hashes[j];
            collection.upsertDocument(collection.getSelfLink(), hashDocument);
        }

        response.setBody(hashes.length);
    });

    if (!result.isAccepted) throw new ("The call was not accepted");

    /**
     * Matched the source with the current hash document
     */
    function match(documents, source) {
        for (var index = 0; index < documents.length; index++) {
            var doc = documents[index];
            if (doc.field === source.field) {
                source.id = doc.id;
                break;
            }
        }
        return source;
    }
}