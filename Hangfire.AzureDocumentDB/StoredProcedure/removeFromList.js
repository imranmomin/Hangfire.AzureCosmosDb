// ReSharper disable UseOfImplicitGlobalInFunctionScope

/**
 * Removes item from list releated to key/value
 * @param {string} key - the key for the set
 * @param {string} value - the value for the key
 */
function removeFromList(key, value) {
    var result = __.filter(function (doc) {
        return doc.type === 5 && doc.key === key && doc.value === value;
    }, function (err, docs) {
        if (err) throw err;

        for (var index = 0; index < docs.length; index++) {
            var isAccepted = __.deleteDocument(docs[index]._self, function (error) {
                if (error) throw error;
            });

            if (!isAccepted) throw new Error("Failed to remove item from list");
        }

        getContext().getResponse().setBody(true);
    });

    if (!result.isAccepted) throw new Error("The call was not accepted");
}