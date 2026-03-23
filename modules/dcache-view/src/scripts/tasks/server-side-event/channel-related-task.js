/**
 * @param {
 *     apiEndpoint: @type {String}
 *      url for the rest api
 *      @default `${window.location.href}api/v1/`
 *
 *     auth: @type {String}
 *      if set and non-empty, the Authorization header
 *      will be set with this value.
 *
 *     method: @type {String}
 *      http method
 *      @default `GET`
 *      @Optional GET | POST | DELETE
 *
 *      url: @type {String}
 *       @Optional
 *
 *      id: @type {String}
 *       @Optional
 *
 *     body: @type {Object}
 *     @Optional
 * }
 */
self.addEventListener('message', function(e) {
    if (!e.data.apiEndpoint && e.data.apiEndpoint === "") {
        e.data.apiEndpoint = `${window.location.href}api/v1/`;
    }
    const header = new Headers({
        "accept": "application/json",
        "content-type": "application/json",
        "suppress-www-authenticate": "Suppress"
    });
    if (e.data.auth && e.data.auth !== "") {
        header.append('Authorization', e.data.auth);
    }

    e.data.method = e.data.method ? e.data.method : 'GET';
    const init = {
        method: e.data.method,
        headers: header,
    };

    if (e.data.method === "POST" && e.data.body && e.data.body !== "") {
        init.body = JSON.stringify(e.data.body);
    }

    switch (e.data.method) {
        case "POST":
            e.data.url = `${e.data.apiEndpoint}events/channels`;
            break;
        case "DELETE":
            if (e.data.id) {
                e.data.url = `${e.data.apiEndpoint}events/channels/${e.data.id}`;
                break;
            }
            if (e.data.url) {
                break;
            }
            throw new ReferenceError("No channel id or url. Please specify either one of the two.");
        case "GET":
            e.data.url = e.data.body && e.data.body !== "" ?
                `${e.data.apiEndpoint}events/channels?client-id=${e.data.body["client-id"]}` :
                `${e.data.apiEndpoint}events/channels`;
            break;
        default:
            throw new ReferenceError(`The http method: ${e.data.method} is not implemented.`);
    }

    fetch(e.data.url, init).then(response => {
        if (!response.ok) {
            throw JSON.stringify({status: response.status, message: response.statusText})
        }
        return e.data.method === "POST" ?
            response.headers.get('location') :
            e.data.method === "DELETE" ? e.data.url : response.json();
    }).then((data) => {
        self.postMessage(data);
    }).catch(error => {
        setTimeout(function(){throw error;});
    })
});