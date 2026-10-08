import type { FileAttributes, FileMetadataRequest } from "./types.ts";

const endpoint: string = "./api/v1/"

export function fileAttributes(r: FileMetadataRequest, headers: Headers): Promise<FileAttributes> {
    const limit = r.limit === 'max' ? 2147483647 : (r.limit ?? 100);
    if (r.pnfsId && r.scope === 'full') {
        return fetchFileAttributes(
            new Request(`${endpoint}id/${r.pnfsId}`, {headers: headers,  credentials: "include"}));
    }
    else if(!r.pnfsId && r.path){
        if (r.scope === "partial") {
            return fetchFileAttributes(new Request(
                `${endpoint}namespace${r.path}?children=true&offset=${r.offset}&limit=${limit}&qos=true`, {
                    headers: headers,  credentials: "include"}));
        }
        return fetchFileAttributes(new Request(
            `${endpoint}namespace${r.path}?children=true&offset=${r.offset}&limit=${limit}&qos=true`, {
                headers: headers,  credentials: "include"}))
            .then(
                atts => {
                    return fileAttributes({ ...r, pnfsId: atts.pnfsId }, headers);
                }
            );
    }
    throw new TypeError("The file object parameter is not set. Provide either file.pnfsId or filePath.")
}

export function fetchFileAttributes(request: Request): Promise<FileAttributes> {
    return fetch(request, {headers: request.headers, credentials: "include"})
        .then((response) => {
            if(!response.ok) {
                throw new Error(`${response.status}: ${response.statusText}`)
            }
            return response.json() as Promise<FileAttributes>;
        });
}

self.onmessage = (e: MessageEvent<FileMetadataRequest>) => {
    const headers = new Headers({
        "Suppress-WWW-Authenticate": "Suppress",
        "Accept": "application/json",
        "Content-Type": "application/json"
    });
    if (e.data.upauth) {
        headers.append("Authorization", e.data.upauth);
    }
    Promise.resolve()
        .then(() => fileAttributes(e.data, headers))
        .then(data => self.postMessage(data))
        .catch(err => { throw err; });
}