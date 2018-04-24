// ReSharper disable UseOfImplicitGlobalInFunctionScope

/**
 * Removes item from list not within the range
 * @param {string} key - the key for the set
 * @param {number} startIndex - the start index
 * @param {number} endIndex - the end index
 */
function trimList(key, startIndex, endIndex) {
    var result = __.filter(function (doc) {
        return doc.type === 5 && doc.key === key;
    }, function (err, docs) {
        if (err) throw err;
        if (docs.length > 0 && docs.length < endIndex) throw new Error("End index is more then the length of the document.");

        for (var index = 0; index < docs.length; index++) {
            if (index < startIndex || index > endIndex) {
                var isAccepted = __.deleteDocument(docs[index]._self, function(error) {
                    if (error) throw error;
                });

                if (!isAccepted) throw new Error("Failed to remove keys");
            }
        }

        getContext().getResponse().setBody(true);
    });

    if (!result.isAccepted) throw new Error("The call was not accepted");
}