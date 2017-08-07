/**
 * Add To Set
 * @param {Object} set - the set information
 * @returns {boolean} true if success; else false 
 */
function addToSet(set) {
    var context = getContext();
    var collection = context.getCollection();
    var response = context.getResponse();

    var result = collection.filter(function (doc) {
        return doc.type === 7 && doc.key === set.key && doc.value === set.value;
    }, function (err, documents) {
        response.setBody(false);
        if (err) throw err;

        if (documents.length > 1) throw new ("Found more than one set for :" + set.key);

        var data;
        if (documents.length === 0) {
            data = set;
        } else {
            data = documents[0];
            data.key = set.key;
            data.value = set.value;
            data.score = set.score;
        }

        collection.upsertDocument(collection.getSelfLink(), data);
        response.setBody(true);
    });

    if (!result.isAccepted) throw new ("The call was not accepted");
}