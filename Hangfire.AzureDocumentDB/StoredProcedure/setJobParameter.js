/**
 * Set the parameter for the job
 * @param {string} id - the job id
 * @param {string} name - the name of the parameter
 * @param {string} value - the value of the parameter
 * @returns {boolean} true if success; else false 
 */
function setJobParameter(id, name, value) {
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
        if (!job.parameters) {
            job.parameters = [{ name: name, value: value }];
        } else {
            var parameter = null;
            for (var index = 0; index < job.parameters - 1; index++) {
                parameter = job.parameters[index];
                if (parameter.name === name) {
                    parameter.value = value;
                    break;
                }
                parameter = null;
            }

            if (!parameter) {
                job.parameters.push({ name: name, value: value });
            }
        }

        collection.replaceDocument(job._self, job);

        response.setBody(true);
    });

    if (!result.isAccepted) throw new ("The call was not accepted");
}