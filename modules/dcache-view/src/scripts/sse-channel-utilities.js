/**
 * Register SSE Channel utilities
 * getAuthValue() method is already in the context.
 * This script contain utilities functions for sse
 */
function listChannelPromise(msg) {
    return new Promise((resolve, reject) => {
        const listChannelWorker = new Worker('scripts/tasks/server-side-event/channel-related-task.js');
        listChannelWorker.addEventListener('message', (e) => {
            listChannelWorker.terminate();
            resolve(e.data);
        });
        listChannelWorker.addEventListener('error', (e) => {
            console.debug(e);
            listChannelWorker.terminate();
            reject(e);
        });
        listChannelWorker.postMessage(msg);
    })
}

function createChannel(msg) {
    return new Promise((resolve, reject) => {
        const createChannelWorker = new Worker('scripts/tasks/server-side-event/channel-related-task.js');
        createChannelWorker.addEventListener('message', function(e) {
            sessionStorage.setItem("sseChannel", e.data);
            console.info("URL of the created channel:");
            console.info(e.data);
            createChannelWorker.terminate();
            changeCurrentSSEStatus(SSEStatus.CREATED);
            resolve(e.data);
        });
        createChannelWorker.addEventListener('error', function(e) {
            console.debug(e);
            createChannelWorker.terminate();
            changeCurrentSSEStatus(SSEStatus.ERROR);
            reject(e);
        });
        msg["method"] = 'POST';
        createChannelWorker.postMessage(msg);
    })
}

function changeCurrentSSEStatus(newStatus) {
    const currentStatus = window.CONFIG["sse"].status;
    if (newStatus !== currentStatus) {
        window.dispatchEvent(new CustomEvent('dv-sse-channel-status-change', {
            detail: {"old": currentStatus, "current": newStatus},
            bubbles: true, composed: true
        }));
        window.CONFIG["sse"].status = newStatus;
    }
}

function deleteChannelPromise(url) {
    console.debug(`Trying to delete channel: ${url}`);
    return new Promise((resolve, reject) => {
        const createChannelWorker = new Worker('scripts/tasks/server-side-event/channel-related-task.js');
        createChannelWorker.addEventListener('message', function(e) {
            sessionStorage.removeItem("sseChannel");
            console.info(`Channel ${e.data} is deleted!`);
            changeCurrentSSEStatus(SSEStatus.CANCELLED);
            resolve(e.data);
            createChannelWorker.terminate();
        });
        createChannelWorker.addEventListener('error', function(e) {
            console.debug("Error occur on channel deletion request.");
            console.debug(e);
            changeCurrentSSEStatus(SSEStatus.ERROR);
            reject(e);
            createChannelWorker.terminate();
        });
        createChannelWorker.postMessage({
            "apiEndpoint": `${window.CONFIG["dcache-view.endpoints.webapi"]}`,
            "auth": getAuthValue(),
            "url": url,
            "method": "DELETE"
        });
    })
}

function watchChannel(address, type) {
    const watchWorker = new Worker('scripts/tasks/server-side-event/receive-events.js');
    watchWorker.addEventListener('message', (e) => {
        console.info(`watching channel ${address} ...`);
        changeCurrentSSEStatus(SSEStatus.RUNNING);
        console.info(e.data);
        window.dispatchEvent(new CustomEvent('dv-sse-events-stream', {
            detail: e.data,
            bubbles: true, composed: true
        }));
    });
    watchWorker.addEventListener('error', function(e) {
        console.debug("Error watching stream of Events");
        console.debug(e);
        if (!!e.message && e.message.includes("Reconnecting")) {
            changeCurrentSSEStatus(SSEStatus.DISCONNECTED);
        } else if (e.message.includes("500")) {
            changeCurrentSSEStatus(SSEStatus.SERVER_ERROR);
        } else {
            changeCurrentSSEStatus(SSEStatus.ERROR);
        }
        watchWorker.terminate();
    });
    watchWorker.postMessage({
        "apiEndpoint": `${window.CONFIG["dcache-view.endpoints.webapi"]}`,
        "auth": getAuthValue(),
        "channel-url": address,
        "type": type
    });
}

async function establishSSEventChannelAsync() {
    console.group('Establishing Server Sent Events Channel');
    const sseWorkerPayloadMsg = {
        "apiEndpoint": `${window.CONFIG["dcache-view.endpoints.webapi"]}`,
        "auth": getAuthValue()
    };
    if (sessionStorage.name && sessionStorage.name !== "") {
        sseWorkerPayloadMsg.body = {"client-id" : `dcache-view-${sessionStorage.name}`};
    }
    if (!!sessionStorage.getItem("sseChannel") && sessionStorage.getItem("sseChannel") !=="") {
        console.info("This client has already created a channel.");
        console.info("Checking whether this channel is still alive...");
        await listChannelPromise(sseWorkerPayloadMsg).then(async (resp) => {
            if (resp.includes(sessionStorage.getItem("sseChannel"))) {
                console.info("Channel is still alive! For now, do nothing.");
                changeCurrentSSEStatus(SSEStatus.CREATED);
            } else {
                console.info("No available channel or the previous channel is dead!");
                console.info("Trying to create a new sse channel ...");
                await createChannel(sseWorkerPayloadMsg);
            }
        }).catch((err) => {
            console.debug(err.message);
            throw new Error(err.message);
        })
    } else {
        console.info("Trying to create a new sse channel ...");
        await createChannel(sseWorkerPayloadMsg);
    }
    console.groupEnd();
    return sessionStorage.getItem("sseChannel");
}

function createSubscription(url, successfulCbFtn, body) {
    const payload = {
        "auth": getAuthValue(),
        "url": url,
        "method": "POST"
    };
    if (body) {
        payload["body"] = body;
    }
    const eventSubscriptionWorker = new Worker('scripts/tasks/server-side-event/subscription-related-task.js');
    eventSubscriptionWorker.addEventListener('message', (e) => {
        successfulCbFtn(e.data);
        eventSubscriptionWorker.terminate();
    });
    eventSubscriptionWorker.addEventListener('error', function(e) {
        console.log(e);
        eventSubscriptionWorker.terminate();
    });
    eventSubscriptionWorker.postMessage(payload);
}

function inotifySubscription(path) {
    createSubscription(
        `${sessionStorage.getItem("sseChannel")}/subscriptions/inotify`,
        data => {
            if (!!data) {
                console.info(`New subscription to inotify events is created.`);
                console.info(`The new subscription address is ${data}`);
                sessionStorage.setItem('newlyInotify', `${encodeURI(path)} ${data}`);
            } else {
                console.info(`Nothing to do here... there is an active subscription based on this path: ${path}`);
            }
        },
        {"path": path})
}

function cancelSubscription(url, eventType) {
    console.log(`Cancelling a subscription ... ${url}`);
    const subscriptionCancellationWorker = new Worker('scripts/tasks/server-side-event/subscription-related-task.js');
    subscriptionCancellationWorker.addEventListener('message', (e) => {
        console.info(`Cancellation of event subscription is ${e.data}!`);
        subscriptionCancellationWorker.terminate();
    });
    subscriptionCancellationWorker.addEventListener('error', (e) => {
        console.info(`Cancellation was unsuccessful!`);
        console.log(e);
        subscriptionCancellationWorker.terminate();
    });
    subscriptionCancellationWorker.postMessage({
        "auth": getAuthValue(),
        "url": url,
        "method": "DELETE",
        "eventType": eventType
    });
}

function initiateSSE() {
    establishSSEventChannelAsync().then((address) => {
        if (!address) {
            throw new Error("Please specify the address of the channel.");
        }
        window.CONFIG["sse"].channel = address;
        watchChannel(address, "inotify");
    }).catch(err => {
        console.debug(err);
    });
}

initiateSSE();