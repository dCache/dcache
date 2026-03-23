self.addEventListener('message', function(e) {
    const header = new Headers({
        "accept": "application/json",
        "content-type": "application/json",
        "suppress-www-authenticate": "Suppress"
    });
    if (e.data.auth && e.data.auth !== "") {
        header.append('Authorization', e.data.auth);
    }

    function getCurrentQoS()
    {
        fetch(`${e.data.endpoint}namespace${e.data.options.path}?qos=true`, {headers: header})
            .then(raw => {return raw.json()})
            .then(file => {
                if (e.data.periodical) {
                    if (file.targetQos !== undefined) {
                        setTimeout(getCurrentQoS(), 2000);
                    } else if (file.currentQos === e.data.options.targetQos
                        || e.data.options.currentQos !== file.currentQos) {
                        self.postMessage({"status" : "successful", "file": file});
                    } else {
                        self.postMessage({"status" : "error", "message": "Terminating Recursion."});
                    }
                } else {
                    self.postMessage({"status" : "successful", "file": file});
                }
            })
            .catch(error => {setTimeout(function(){throw error;});});
    }

    getCurrentQoS();
}, false);