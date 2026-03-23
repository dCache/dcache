"use strict";
/**
 * @param {
 *     url: @type {String}
 *          file WebDav door endpoint url
 *
 *     body: @type {Object}
 *          see macaroon doc.
 *
 *     upauth: @type {String}
 *          if set and non-empty, the Authorization header
 *          will be set with this value.
 * }
 */
self.addEventListener('message', function(e) {
    const body = JSON.stringify(e.data.body);
    const headers = new Headers({
        "Suppress-WWW-Authenticate": "Suppress",
        "Content-Type": "application/macaroon-request"
    });
    if (e.data.upauth && e.data.upauth !== "") {
        headers.append("Authorization", `${e.data.upauth}`);
    }
    if (!e.data.url) {
        throw new TypeError('The WebDav door endpoint url is not provided.');
    }
    fetch(e.data.url, {
        method: 'POST',
        mode: "cors",
        credentials: "include",
        body: body,
        headers: headers
    }).then((response) => {
        if(response.ok) {
            return response.json();
        }
        throw new Error('Network response was not ok: ' + response.statusText);
    }).then((rep) => {
        self.postMessage(rep);
    }).catch(function(err) {
        //WORKAROUND: https://stackoverflow.com/questions/30715367/why-can-i-not-throw-inside-a-promise-catch-handler
        setTimeout(function() { throw err; });
    });
}, false);
