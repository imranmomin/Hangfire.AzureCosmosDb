// ReSharper disable UseOfImplicitGlobalInFunctionScope

/**
 * Expires a job
 * @param {string} id - the job id
 */
function persistJob(id) {
    var result = __.filter(function (doc) { return doc.id === id; }, function (err, docs) {
        if (err) throw err;
        if (docs.length === 0) throw new Error("No job found for id :" + id);
        if (docs.length > 1) throw new Error("Found more than one job for id :" + id);

        var doc = docs[0];
        if (doc.expire_on) {
            delete doc.expire_on;

            var isAccepted = __.replaceDocument(doc._self, doc, function (error) {
                if (error) throw error;
            });

            if (!isAccepted) throw new Error("Failed to presist the job");
        }

        getContext().getResponse().setBody(true);
    });

    if (!result.isAccepted) throw new Error("The call was not accepted");
}