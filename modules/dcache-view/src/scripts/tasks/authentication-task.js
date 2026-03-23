fetch('/api/v1/user', {
    headers: {
        "Suppress-WWW-Authenticate": "Suppress",
        "Accept": "Application/json"
    }})
    .then(function(response) {
        if (response.status !== 200) {
            throw new Error(`Looks like there was a problem. Status Code: ${response.status}`);
        }
        return response.json();
    })
    .then(function(user) {
        if (user.status === "AUTHENTICATED") {
            self.postMessage(user);
        }
    })
    .catch(function (err) {
        setTimeout(function(){throw new Error(err.message);});
    });
