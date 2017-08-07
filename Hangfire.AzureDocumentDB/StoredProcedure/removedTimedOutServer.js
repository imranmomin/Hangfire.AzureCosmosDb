/**
 * Remove TimedOut Server
 * @param {string} id - the server id
 * @returns {number} number of servers removed 
 */
function removedTimedOutServer(lastHeartbeat) {
    var context = getContext();
    var collection = context.getCollection();
    var response = context.getResponse();

    var result = collection.filter(function (doc) {
        return doc.type === 1 && doc.last_heartbeat < lastHeartbeat;
    }, function (err, documents) {
        response.setBody(0);
        if (err) throw err;

        for (var index = 0; index < documents.length; index++) {
            var self = documents[0]._self;
            collection.deleteDocument(self);
        }
        
        response.setBody(documents.length);
    });

    if (!result.isAccepted) throw new ("The call was not accepted");
}