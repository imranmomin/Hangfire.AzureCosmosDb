// ReSharper disable UseOfImplicitGlobalInFunctionScope

/**
 * Expires a job
 * @param {string} id - the job id
 * @param {Object} state - the state information for the job
 */
function setJobState(id, state) {
    var result = __.filter(function (doc) {
        return doc.id === id && doc.type === 2;
    }, function (err, docs) {
        if (err) throw err;
        if (docs.length === 0) throw new Error("No job found for id :" + id);
        if (docs.length > 1) throw new Error("Found more than one job for id :" + id);

        // create the state document
        createState(state, function (document) {
            var doc = docs[0];
            doc.state_id = document.id;
            doc.state_name = state.name;

            var isAccepted = __.replaceDocument(doc._self, doc, function (error) {
                if (error) throw error;
            });

            if (!isAccepted) throw new Error("Failed to set the state to job");
            else getContext().getResponse().setBody(true);

        });
    });

    if (!result.isAccepted) throw new Error("The call was not accepted");

    /**
     * Creates a new state
     * @param {Object} state - information for the job 
     * @param {function} callback - return the newly create state document
     */
    function createState(state, callback) {
        __.createDocument(__.getSelfLink(), state, function (err, document) {
            if (err) throw err;
            callback(document);
        });
    }
}