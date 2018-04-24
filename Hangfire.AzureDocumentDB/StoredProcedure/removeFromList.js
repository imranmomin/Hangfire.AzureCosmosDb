// ReSharper disable UseOfImplicitGlobalInFunctionScope

/**
 * Removes item from list releated to key/value
 * @param {string} key - the key for the set
 * @param {Object} value - the value for the key
 */
function removeFromList(key, value) {
    var result = __.filter(function (doc) {
        return doc.type === 5 && doc.key === key && doc.value === value;
    }, function (err, docs) {
        if (err) throw err;
        if (docs.length === 0) throw new Error("No item found for key :" + key);
        if (docs.length > 1) throw new Error("Found more than item found for key :" + key);

        var isAccepted = __.deleteDocument(docs[0]._self, function (error) {
            if (error) throw error;
        });

        if (!isAccepted) throw new Error("Failed to remove item from list");
        else getContext().getResponse().setBody(true);
    });

    if (!result.isAccepted) throw new Error("The call was not accepted");
}