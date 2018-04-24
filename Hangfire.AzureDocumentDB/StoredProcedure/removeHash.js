// ReSharper disable UseOfImplicitGlobalInFunctionScope

/**
 * Remove key from Hash
 * @param {string} key - the key for the set
 */
function removeHash(key) {
    var result = __.filter(function (doc) {
        return doc.type === 6 && doc.key === key;
    }, function (err, docs) {
        if (err) throw err;

        for (var index = 0; index < docs.length; index++) {
            var isAccepted = __.deleteDocument(docs[index]._self, function(error) {
                if (error) throw error;
            });

            if (!isAccepted) throw new Error("Failed to remove key from hash");
        }

        getContext().getResponse().setBody(true);
    });

    if (!result.isAccepted) throw new Error("The call was not accepted");
}