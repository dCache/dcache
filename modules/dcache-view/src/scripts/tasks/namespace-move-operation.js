/**
 * Move or rename operation.
 *
 * @param {
 *     endpoint: @type {String} @required
 *          url endpoint of dCache restful api
 *
 *     upauth: @type {String} @required
 *          if set and non-empty, the Authorization header
 *          will be set with this value.
 *
 *     source: @type {String} @required
 *          absolute path of the file location.
 *
 *     destination: @type {String} @required
 *          absolute path where the file should be moved to.
 * }
 */
self.addEventListener('message', function(e) {
    if (!(e.data.endpoint && e.data.upauth && e.data.source && e.data.destination)) {
        throw new TypeError('One or more of the required parameters is not provided.');
    }
    const headers = new Headers({
        "Suppress-WWW-Authenticate": "Suppress",
        "Accept": "application/json",
        "Content-Type": "application/json"
    });
    if (e.data.upauth && e.data.upauth !== "") {
        headers.append("Authorization", `${e.data.upauth}`);
    }
    fetch(new Request(`${e.data.endpoint}namespace${e.data.source}`, {
        method: "POST",
        headers: headers,
        body: JSON.stringify({"action": "mv", "destination": e.data.destination})
    })).then(response => {
        if (!response.ok) {
            throw JSON.stringify({status: response.status, message: response.statusText})
        }
        return response.json();
    }).then(successful => {
        self.postMessage(successful);
    }).catch(error => {
        setTimeout(function() { throw error; });
    })
}, false);