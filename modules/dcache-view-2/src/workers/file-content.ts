import type { FileContentRequest, FileContent } from "./types.ts";

export function buildRequest(e: MessageEvent<FileContentRequest>): Request {
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

export async function processResponse(response: Response, e: MessageEvent<FileContentRequest>)
    : Promise<FileContent | undefined> {
    console.log("download the file " + response.url);

    if (response.ok) {
        const data: Blob | unknown = e.data.mime.includes('json') ? await response.json() : await response.blob();
        return {data: data} as FileContent;
    } else if (response.status >= 400 && response.status < 500) {
        throw new Error(`Request failed with response status code ${response.status}.`);
    } else if (response.status >= 500) {
        throw new Error(`Status code ${response.status} - dCache Internal Server Error. Please contact the admin.`);
    }
}

self.onmessage = (e: MessageEvent<FileContentRequest>) => {
    fetch(buildRequest(e))
        .then(response => processResponse(response, e))
        .then(data => self.postMessage(data));
};