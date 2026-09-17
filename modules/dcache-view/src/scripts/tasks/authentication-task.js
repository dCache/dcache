fetch('/api/v1/user', {
    credentials: 'include',   //  send session cookie
    headers: {
        "Suppress-WWW-Authenticate": "Suppress",
        "Accept": "Application/json"
    }})
    .then(function(response) {
        console.log("IS USER RESOURCE STILL CALLED ");

        if (response.status !== 200) {
            throw new Error(`Looks like there was a problem. Status Code: ${response.status}`);
        }
        return response.json();
    })
    .then(function(user) {
        console.log("AUTHENTICATED user resource: ");
        if (user.status === "AUTHENTICATED") {
            self.postMessage(user);
        }
    })
    .catch(function (err) {
        setTimeout(function(){throw new Error(err.message);});
    });
