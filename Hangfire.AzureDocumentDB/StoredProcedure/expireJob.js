// ReSharper disable UseOfImplicitGlobalInFunctionScope

/**
 * Expires a job
 * @param {string} id - the job id
 * @param {int} expireOn - the unix time when the job expires
 */
function expireJob(id, expireOn) {
    var result = __.filter(function (doc) {
        return doc.type === 2 && doc.id === id;
    }, function (err, docs) {
        if (err) throw err;
        if (docs.length === 0) throw new Error("No job found for id :" + id);
        if (docs.length > 1) throw new Error("Found more than one job for id :" + id);

        var doc = docs[0];
        doc.expire_on = expireOn;

        var isAccepted = __.replaceDocument(doc._self, doc, function (error) {
            if (error) throw error;
        });

        if (!isAccepted) throw new Error("Failed to expire the job");
        else getContext().getResponse().setBody(true);
    });

    if (!result.isAccepted) throw new Error("The call was not accepted");
}