self.addEventListener('message', function(e) {

    const headers = new Headers({
        "Suppress-WWW-Authenticate": "Suppress",
        "Content-Type": e.data.mime
    });
    if (e.data.upauth && e.data.upauth !== "") {
        headers.append("Authorization", `${e.data.upauth}`);
    }
    const request = new Request(e.data.url, {
        headers: headers,
        mode: "cors",
        redirect: "follow",
        credentials: "include"
    });

    fetch(request).then(file => {
        if (file.ok) {
            return e.data.return === 'json' ? file.json() : file.blob();
        }
        if (file.status >= 400 && file.status < 500) {
            throw new TypeError(`Request failed with response status code ${file.status}.`);
        }
        if (file.status >= 500) {
            throw new Error(`Status code ${file.status} - dCache Internal Server Error. Please contact the admin.`);
        }
    }).then(data => {
        self.postMessage(data);
    }).catch(error => {setTimeout(function(){throw error;});})
}, false);