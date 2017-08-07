/**
 * Remove Server
 * @param {string} id - the server id
 * @returns {boolean} true if success; else false 
 */
function removeServer(id) {
    var context = getContext();
    var collection = context.getCollection();
    var response = context.getResponse();

    var result = collection.filter(function (doc) {
        return doc.type === 1 && doc.server_id === id;
    }, function (err, documents) {
        response.setBody(false);
        if (err) throw err;

        if (documents.length > 1) throw new ("Found more than one server for :" + id);

        var self = documents[0]._self;
        collection.deleteDocument(self);
        response.setBody(true);
    });

    if (!result.isAccepted) throw new ("The call was not accepted");
}