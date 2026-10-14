import type { FileContentRequest, FileContent } from "../types/types.ts";

function buildRequest(r: FileContentRequest): Request {
    const headers = new Headers({
        "Suppress-WWW-Authenticate": "Suppress",
        "Content-Type": r.mime
    });

    if (r.upauth && r.upauth !== "") {
        headers.append("Authorization", r.upauth);
    }

    const request = new Request(r.url, {
        headers,
        mode: "cors",
        redirect: "follow",
        credentials: "include"
    });
    return request;
}

export async function processResponse(response: Response, r: FileContentRequest)
    : Promise<FileContent> {
    console.log("download the file " + response.url);

    if (response.ok) {
        const data: Blob | unknown = r.mime.includes('json') ? await response.json() : await response.blob();
        return {data: data} as FileContent;
    } else if (response.status >= 400 && response.status < 500) {
        throw new Error(`Request failed with response status code ${response.status}.`);
    } else if (response.status >= 500) {
        throw new Error(`Status code ${response.status} - dCache Internal Server Error. Please contact the admin.`);
    } else {
        throw new Error(`Unexpected response status code ${response.status}.`);
    }
}

export async function fileContent(r: FileContentRequest): Promise<FileContent> {
    return await fetch(buildRequest(r))
        .then(response => processResponse(response, r))
}