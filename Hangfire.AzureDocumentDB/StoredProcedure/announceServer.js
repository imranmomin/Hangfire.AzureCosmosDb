// ReSharper disable UseOfImplicitGlobalInFunctionScope

/**
 * Announce server
 * @param {Object} server - the server information
 */
function announceServer(server) {
    var result = __.filter(function (doc) {
        return doc.type === 1 && doc.server_id === server.server_id;
    }, function (err, docs) {
        if (err) throw err;
        if (docs.length > 1) throw new Error("Found more than one server for :" + server.server_id);

        var doc;
        if (docs.length === 0) {
            doc = server;
        } else {
            doc = docs[0];
            doc.last_heartbeat = server.last_heartbeat;
            doc.workers = server.workers;
            doc.queues = server.queues;
        }

        var isAccepted = __.upsertDocument(__.getSelfLink(), doc, function(error) {
            if (error) throw error;
        });

        if (!isAccepted) throw new Error("Failed to create a server document");
        else getContext().getResponse().setBody(true);
    });

    if (!result.isAccepted) throw new Error("The call was not accepted");
}