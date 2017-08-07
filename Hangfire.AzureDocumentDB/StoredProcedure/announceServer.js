/**
 * Announce server
 * @param {Object} server - the server information
 * @returns {boolean} true if success; else false 
 */
function announceServer(server) {
    var context = getContext();
    var collection = context.getCollection();
    var response = context.getResponse();

    var result = collection.filter(function (doc) {
        return doc.type === 1 && doc.server_id === server.server_id;
    }, function (err, documents) {
        response.setBody(false);
        if (err) throw err;

        if (documents.length > 1) throw new ("Found more than one server for :" + server.server_id);

        var data;
        if (documents.length === 0) {
            data = server;
        } else {
            data = documents[0];
            data.last_heartbeat = set.last_heartbeat;
            data.workers = set.workers;
            data.queues = set.queues;
        }

        collection.upsertDocument(collection.getSelfLink(), data);
        response.setBody(true);
    });

    if (!result.isAccepted) throw new ("The call was not accepted");
}