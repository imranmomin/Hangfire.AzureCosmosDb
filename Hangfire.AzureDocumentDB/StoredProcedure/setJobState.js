/**
 * Expires a job
 * @param {string} id - the job id
 * @param {Object} state - the state information for the job
 * @returns {boolean} true if success; else false 
 */
function setJobState(id, state) {
    var context = getContext();
    var collection = context.getCollection();
    var response = context.getResponse();

    var result = collection.filter(function (doc) { return doc.id === id && doc.type === 2; }, function (err, documents) {
        response.setBody(false);
        if (err) throw err;

        if (documents.length === 0) throw new ("No job found for id :" + id);
        if (documents.length > 1) throw new ("Found more than one job for id :" + id);

        // create the state document
        createState(state, function (document) {
            var job = documents[0];
            job.state_id = document.id;
            job.state_name = state.name;
            collection.replaceDocument(job._self, job);
        });

        response.setBody(true);
    });

    if (!result.isAccepted) throw new ("The call was not accepted");

    /**
     * Creates a new state
     * @param {Object} state - information for the job 
     * @param {function} callback - return the newly create state document
     */
    function createState(state, callback) {
        collection.createDocument(collection.getSelfLink(), state, function (err, document) {
            if (err) throw err;
            callback(document);
        });
    }
}