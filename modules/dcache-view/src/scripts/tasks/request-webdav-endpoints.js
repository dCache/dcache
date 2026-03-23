self.addEventListener('message', function(e) {
    const endpoint = e.data.apiEndpoint.endsWith("/") ? e.data.apiEndpoint : `${e.data.apiEndpoint}/`;
    const header = new Headers({
        "accept": "application/json",
        "content-type": "application/json",
        "suppress-www-authenticate": "Suppress"
    });
    if (e.data.auth && e.data.auth !== "") {
        header.append('Authorization', e.data.auth);
    }
    fetch(`${endpoint}doors`, {headers: header})
        .then((response) => {
            if (!(response.status >= 200 && response.status < 300)) {
                throw new Error("Network problem.");
            }
            return response.json();
        })
        .then(doors => {
            const read = [];
            const write = [];
            const webdav = [];
            doors.forEach((door) => {
                if (door.tags && door.tags.includes("dcache-view") && door.root === "/") {
                    webdav.push(door);
                }
            });
            webdav.sort(compare);

            webdav.forEach((door) => {
                door.readPaths.forEach((path) => {
                    read.push(`${door.protocol}://${door.addresses[0]}:${door.port}${path}`);
                });

                door.writePaths.forEach((path) => {
                    write.push(`${door.protocol}://${door.addresses[0]}:${door.port}${path}`);
                });
            });

            self.postMessage({"write": write, "read": read, "timestamp": new Date().getTime()})
        })
        .catch(e => {
            setTimeout(function(){throw e;});
        });
    function compare(door_1, door_2) {
        const condition_1 = e.data.protocol === door_1.protocol;
        const condition_2 = e.data.protocol === door_2.protocol;
        if ((condition_1 && condition_2) || (!condition_1 && !condition_2)) {
            compare_by_load(door_1, door_2)
        }
        if (condition_1 && !condition_2) {
            return -1
        }
        if (!condition_1 && condition_2) {
            return 1
        }
    }
    function compare_by_load(door_1, door_2) {
        return door_1.load === door_2.load ? 0 : (door_1.load < door_2.load ? -1  : 1);
    }
}, false);