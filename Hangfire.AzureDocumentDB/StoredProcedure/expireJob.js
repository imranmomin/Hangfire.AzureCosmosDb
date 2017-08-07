/**
 * Expires a job
 * @param {string} id - the job id
 * @param {int} expireOn - the unix time when the job expires
 * @returns {boolean} true if expired; else false 
 */
function expireJob(id, expireOn) {
    var context = getContext();
    var collection = context.getCollection();
    var response = context.getResponse();

    var result = collection.filter(function (doc) {
        return doc.type === 2 && doc.id === id;
    }, function (err, documents) {
        response.setBody(false);
        if (err) throw err;

        if (documents.length === 0) throw new ("No job found for id :" + id);
        if (documents.length > 1) throw new ("Found more than one job for id :" + id);

        var job = documents[0];
        job.expire_on = expireOn;
        collection.replaceDocument(job._self, job);

        response.setBody(true);
    });

    if (!result.isAccepted) throw new ("The call was not accepted");
}