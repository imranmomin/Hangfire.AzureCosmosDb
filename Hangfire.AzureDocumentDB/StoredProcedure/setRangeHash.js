// ReSharper disable UseOfImplicitGlobalInFunctionScope

/**
 * Remove key from Hash
 * @param {string} key - the key for the set
 * @param {Array<Object>} sources - the array of hash
 */
function setRangeHash(key, sources) {
    var result = __.filter(function (doc) {
        return doc.type === 6 && doc.key === key;
    }, function (err, docs) {
        if (err) throw err;

        var hashes = [];
        for (var index = 0; index < sources.length; index++) {
            var hash = match(docs, sources[index]);
            hashes.push(hash);
        }

        // upsert all the hash documents
        for (var j = 0; j < hashes.length; j++) {
            var doc = hashes[j];
            var isAccepted = __.upsertDocument(__.getSelfLink(), doc, function(error) {
                if (error) throw error;
            });

            if (!isAccepted) throw new Error("Failed to save hashes");
        }

        getContext().getResponse().setBody(hashes.length);
    });

    if (!result.isAccepted) throw new Error("The call was not accepted");

    /**
     * Matched the source with the current hash document
     */
    function match(docs, source) {
        for (var index = 0; index < docs.length; index++) {
            var doc = docs[index];
            if (doc.field === source.field && doc.key === source.key) {
                source.id = doc.id;
                break;
            }
        }
        return source;
    }
}