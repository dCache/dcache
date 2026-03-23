self.addEventListener('message', function(e) {
    if (!e.data.url && e.data.url === "") {
        throw new ReferenceError("Please specify url");
    }
    const header = new Headers({
        "accept": "application/json",
        "content-type": "application/json",
        "suppress-www-authenticate": "Suppress"
    });
    if (e.data.auth && e.data.auth !== "") {
        header.append('Authorization', e.data.auth);
    }
    const init = {headers: header};
    switch (e.data.method) {
        case "POST":
            if (e.data.body && e.data.body !== "") {
                try {
                    init.body = JSON.stringify(e.data.body);
                } catch (e) {
                    throw new ReferenceError("Malformed body. Please check the body of your request.");
                }
            }
            break;
        case "DELETE":
            const splitSubscriptionUrlArray = e.data.url.split("/");
            if (!(splitSubscriptionUrlArray[splitSubscriptionUrlArray.length - 2] === e.data.eventType)) {
                throw new ReferenceError("Delete cannot be send because the subscription's url is incorrect.");
            }
            break;
    }
    init.method = e.data.method ? e.data.method : "GET";

    fetch(e.data.url, init).then(response => {
        if (!response.ok) {
            throw JSON.stringify({status: response.status, message: response.statusText})
        }
        self.postMessage(e.data.method === 'POST' ? response.headers.get('location') : "done");
    }).catch(error => {
        setTimeout(function(){throw error;});
    })
});