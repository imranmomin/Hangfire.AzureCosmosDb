function removeServer(id) {
    let context = getContext();
    let collection = context.getCollection();
    let collectionLink = collection.getSelfLink();
    let response = getContext().getResponse();
    let documentLink = `${collectionLink}/docs/${id}`;
    response.setBody(false);
    let result = collection.deleteDocument(documentLink, (error) => {
        if (error) {
            throw error;
        }
        response.setBody(true);
    });
    if (!result) {
        throw new Error("The call was not accepted");
    }
}
