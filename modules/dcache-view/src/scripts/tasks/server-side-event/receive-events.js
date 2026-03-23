importScripts('../../lazy-loads/dcache-eventsource.js');
self.addEventListener('message', function(e) {
    if (!e.data["channel-url"] && e.data["channel-url"] === "") {
        throw new ReferenceError("Please specify either channel-id or channel-url");
    }
    const init = {};
    if (e.data.auth && e.data.auth !== "") init["auth"] = e.data.auth;

    const source = new dCacheEventSource(e.data["channel-url"], init);

    source.addEventListener(e.data.type, (event) => {
        const dt = {"type": event.type, "data": JSON.parse(event.detail.data)};
        console.debug(dt);
        self.postMessage(dt);
    });
    source.addEventListener("SYSTEM", (event) => {
        const dt = {"type": event.type, "data": JSON.parse(event.detail.data)};
        console.debug(dt);
        self.postMessage(dt);
    });
    source.addEventListener('error', (event)=> {
        console.debug("Error with SSE connected");
        console.debug('%O', event);
        source.close();
        throw new Error(event.detail.message);
    })
});