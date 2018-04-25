// ReSharper disable UseOfImplicitGlobalInFunctionScope

/**
 * Heartbest Server
 * @param {string} id - the server id
 * @param {number} heartbeat = the epoc time
 */
function heartbeatServer(id, heartbeat) {
    var result = __.filter(function (doc) {
        return doc.type === 1 && doc.server_id === id;
    }, function (err, docs) {
        if (err) throw err;
        if (docs.length === 0) throw new Error("No server found for id :" + id);
        if (docs.length > 1) throw new Error("Found more than one server for :" + id);

        var doc = docs[0];
        doc.last_heartbeat = heartbeat;

        var isAccepted = __.replaceDocument(doc._self, doc, function(error) {
            if (error) throw error;
        });

        if (!isAccepted) throw new Error("Failed to update the sever heartbeat");
        else getContext().getResponse().setBody(true);
    });

    if (!result.isAccepted) throw new Error("The call was not accepted");
}