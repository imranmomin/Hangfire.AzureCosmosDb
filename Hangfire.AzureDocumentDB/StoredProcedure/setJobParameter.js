// ReSharper disable UseOfImplicitGlobalInFunctionScope

/**
 * Set the parameter for the job
 * @param {string} id - the job id
 * @param {string} name - the name of the parameter
 * @param {string} value - the value of the parameter
 */
function setJobParameter(id, name, value) {
    var result = __.filter(function (doc) {
        return doc.type === 2 && doc.id === id;
    }, function (err, docs) {
        if (err) throw err;
        if (docs.length === 0) throw new Error("No job found for id :" + id);
        if (docs.length > 1) throw new Error("Found more than one job for id :" + id);

        var doc = docs[0];
        if (!doc.parameters) {
            doc.parameters = [{ name: name, value: value }];
        } else {
            var parameter = null;
            for (var index = 0; index < doc.parameters - 1; index++) {
                parameter = doc.parameters[index];
                if (parameter.name === name) {
                    parameter.value = value;
                    break;
                }
                parameter = null;
            }

            if (!parameter) {
                doc.parameters.push({ name: name, value: value });
            }
        }

        var isAccepted = __.replaceDocument(doc._self, doc, function(error) {
            if (error) throw error;
        });

        if (!isAccepted) throw new Error("Failed to set the parameter");
        else getContext().getResponse().setBody(true);
    });

    if (!result.isAccepted) throw new Error("The call was not accepted");
}