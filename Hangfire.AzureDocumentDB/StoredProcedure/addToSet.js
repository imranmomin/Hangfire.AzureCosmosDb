// ReSharper disable UseOfImplicitGlobalInFunctionScope

/**
 * Add To Set
 * @param {Object} set - the set information
 */
function addToSet(set) {
    var result = __.filter(function (doc) {
        return doc.type === 7 && doc.key === set.key && doc.value === set.value;
    }, function (err, docs) {
        if (err) throw err;
        if (docs.length > 1) throw new Error("Found more than one set for :" + set.key);

        var doc;
        if (docs.length === 0) {
            doc = set;
        } else {
            doc = docs[0];
            doc.score = set.score;
        }

        var isAccepted = __.upsertDocument(__.getSelfLink(), doc, function (error) {
            if (error) throw error;
        });
        
        if (!isAccepted) throw new Error("Failed to create a set document");
        else getContext().getResponse().setBody(true);
    });

    if (!result.isAccepted) throw new Error("The call was not accepted");
}