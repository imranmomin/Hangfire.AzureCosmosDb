// ReSharper disable UseOfImplicitGlobalInFunctionScope

/**
 * Remove Server
 * @param {string} id - the server id
 */
function removeServer(id) {
    var result = __.filter(function (doc) {
        return doc.type === 1 && doc.server_id === id;
    }, function (err, docs) {
        if (err) throw err;
        if (docs.length === 0) throw new Error("No server found for id :" + id);
        if (docs.length > 1) throw new Error("Found more than one server for id :" + id);

        var isAccepted = __.deleteDocument(docs[0]._self, function (error) {
            if (error) throw error;
        });

        if (!isAccepted) throw new Error("Failed to remove the server");
        else getContext().getResponse().setBody(true);
    });

    if (!result.isAccepted) throw new Error("The call was not accepted");
}