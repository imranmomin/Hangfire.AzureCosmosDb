/**
 * Heartbest Server
 * @param {string} id - the server id
 * @param {number} heartbeat = the epoc time
 * @returns {boolean} true if success; else false 
 */
function heartbeatServer(id, heartbeat) {
    var context = getContext();
    var collection = context.getCollection();
    var response = context.getResponse();

    var result = collection.filter(function (doc) {
        return doc.type === 1 && doc.server_id === id;
    }, function (err, documents) {
        response.setBody(false);
        if (err) throw err;

        if (documents.length === 0) throw new ("No server found for id :" + id);
        if (documents.length > 1) throw new ("Found more than one server for :" + id);

        var server = documents[0];
        server.last_heartbeat = heartbeat;
        collection.replaceDocument(server._self, server);
        response.setBody(true);
    });

    if (!result.isAccepted) throw new ("The call was not accepted");
}