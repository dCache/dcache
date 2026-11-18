/**
 * Uploads a file to WebDAV with a PUT.
 * upauth is the full header value ("Basic ..."), or undefined for OIDC (cookie) and anonymous.
 * onProgress gets the uploaded percentage (0-100). Throws on error.
 */
export function uploadFile(url: string, file: File, upauth?: string,
                           onProgress?: (percent: number) => void): Promise<void> {
    return new Promise((resolve, reject) => {
        const xhr = new XMLHttpRequest();
        xhr.open("PUT", url);
        xhr.withCredentials = true;
        xhr.setRequestHeader("Content-Type", file.type || "application/octet-stream");
        xhr.setRequestHeader("Suppress-WWW-Authenticate", "Suppress");
        if (upauth) {
            xhr.setRequestHeader("Authorization", upauth);
        }
        // fetch cannot report upload progress, XMLHttpRequest can
        xhr.upload.addEventListener("progress", e => {
            if (e.lengthComputable) onProgress?.(Math.round(e.loaded / e.total * 100));
        });
        xhr.onload = () => xhr.status === 200 || xhr.status === 201
            ? resolve()
            : reject(new Error(`Upload failed with response status code ${xhr.status}.`));
        xhr.onerror = () => reject(new Error("Upload failed: network error."));
        xhr.send(file);
    });
}
