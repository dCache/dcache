import type { WorkerMessageData, WorkerResponse } from "./types";

export function buildRequest(e: MessageEvent<WorkerMessageData>): Request {
    const headers = new Headers({
        "Suppress-WWW-Authenticate": "Suppress",
        "Content-Type": e.data.mime
    });

    if (e.data.upauth && e.data.upauth !== "") {
        headers.append("Authorization", e.data.upauth);
    }

    const request = new Request(e.data.url, {
        headers,
        mode: "cors",
        redirect: "follow",
        credentials: "include"
    });
    return request;
}

export async function processResponse(response: Response, e: MessageEvent<WorkerMessageData>)
    : Promise<WorkerResponse | undefined> {
    console.log("download the file " + response.url);

    if (response.ok) {
        console.log("download is possible " + e.data.return);
        const data = e.data.return === 'json' ? await response.json() : await response.blob();
        return {data: data} as WorkerResponse;
    } else if (response.status >= 400 && response.status < 500) {
        throw new Error(`Request failed with response status code ${response.status}.`);
    } else if (response.status >= 500) {
        throw new Error(`Status code ${response.status} - dCache Internal Server Error. Please contact the admin.`);
    }
}

self.addEventListener('message', (e: MessageEvent<WorkerMessageData>) => {

    fetch(buildRequest(e))
        .then(response => processResponse(response, e))
        .then(data => self.postMessage(data));

}, false);