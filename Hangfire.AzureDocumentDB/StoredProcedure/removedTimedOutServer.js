// ReSharper disable UseOfImplicitGlobalInFunctionScope

/**
 * Remove TimedOut Server
 * @param {string} id - the server id
 */
function removedTimedOutServer(lastHeartbeat) {
    var result = __.filter(function (doc) {
        return doc.type === 1 && doc.last_heartbeat < lastHeartbeat;
    }, function (err, docs) {
        if (err) throw err;

        var removed = 0;
        for (var index = 0; index < docs.length; index++) {
            var doc = docs[index];

            var isAccepted = __.deleteDocument(doc._self, function (error) {
                if (error) throw error;
            });

            if (!isAccepted) throw new Error("Failed to remove timeout server");
            else removed += 1;
        }

        getContext().getResponse().setBody(removed);
    });

    if (!result.isAccepted) throw new Error("The call was not accepted");
}