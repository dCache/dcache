import type {FileAttributes, FileMetadataRequest} from "../types/types.ts";

const endpoint: string = "./api/v1/"

export async function fileAttributes(r: FileMetadataRequest, headers: Headers): Promise<FileAttributes> {
    const limit = r.limit === 'max' ? 2147483647 : (r.limit ?? 100);
    if (r.pnfsId && r.scope === 'full') {
        return fetchFileAttributes(
            new Request(`${endpoint}id/${r.pnfsId}`, {headers: headers, credentials: "include"}));
    } else if (!r.pnfsId && r.path) {
        if (r.scope === "partial") {
            return fetchFileAttributes(new Request(
                `${endpoint}namespace${r.path}?children=true&offset=${r.offset}&limit=${limit}`, {
                    headers: headers, credentials: "include"
                }));
        }
        const atts = await fetchFileAttributes(new Request(
            `${endpoint}namespace${r.path}?children=true&offset=${r.offset}&limit=${limit}`, {
                headers: headers, credentials: "include"
            }));
        return await fileAttributes({...r, pnfsId: atts.pnfsId}, headers);
    }
    throw new TypeError("The file object parameter is not set. Provide either file.pnfsId or filePath.")
}

export function ls(file: FileAttributes): FileAttributes[]{
    if (file.fileType !== 'DIR') {
        throw new TypeError(file.fileName + " is not a directory");
    } else if (!file.children || file.children.length === 0) {
        return [];
    }
    return file.children;
}

async function fetchFileAttributes(request: Request): Promise<FileAttributes> {
    const response = await fetch(request, {headers: request.headers, credentials: "include"});
    if (!response.ok) {
        throw new Error(`${response.status}: ${response.statusText}`);
    }
    return await response.json() as Promise<FileAttributes>;
}

export function buildHeaders(upauth?: string) {
    const headers = new Headers({
        "Suppress-WWW-Authenticate": "Suppress",
        "Accept": "application/json",
        "Content-Type": "application/json"
    });
    if (upauth) {
        headers.append("Authorization", upauth);
    }
    return headers;
}