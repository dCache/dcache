/** Starts the browser download of a URL with a plain link, so the file does not go through memory. */
export function startDownload(url: string): void {
    const link = document.createElement("a");
    link.href = url;
    link.download = "";
    link.click();
}

/**
 * Asks the WebDAV door for a short-lived macaroon (10 minutes, download only) for the file.
 * upauth is the full header value ("Basic ..."). Returns the file URL with the macaroon in it.
 */
export async function requestMacaroon(url: string, upauth: string): Promise<string> {
    const response = await fetch(url, {
        method: "POST",
        mode: "cors",
        credentials: "include",
        headers: {
            "Suppress-WWW-Authenticate": "Suppress",
            "Content-Type": "application/macaroon-request",
            "Authorization": upauth
        },
        body: JSON.stringify({ caveats: ["activity:DOWNLOAD"], validity: "PT10M" })
    });
    if (!response.ok) {
        throw new Error(`Macaroon request failed with response status code ${response.status}.`);
    }
    const { uri } = await response.json() as { uri: { targetWithMacaroon: string } };
    return uri.targetWithMacaroon;
}

/**
 * Downloads a file from WebDAV.
 * Without upauth (OIDC cookie, anonymous): plain link to the file.
 * With upauth ("Basic ..."): a link cannot send the password, so a macaroon is requested first.
 */
export async function downloadFile(url: string, upauth?: string): Promise<void> {
    startDownload(upauth ? await requestMacaroon(url, upauth) : url);
}
