import type { ViewConfig } from "../types/types.ts";

/** Base URL of the WebDAV door: dcache-view.endpoints.webdav from config.js,
 * or <same host>:2880 if not set (same fallback as the old dcache-view). */
export function webdavEndpoint(conf: ViewConfig, location: Location = window.location): string {
    const configured = conf["dcache-view.endpoints.webdav"]?.trim();
    return configured ? configured : `${location.protocol}//${location.hostname}:2880`;
}

/** Full WebDAV URL of a file. path must be absolute, each part is URL-encoded. */
export function webdavFileUrl(base: string, path: string): string {
    if (!path.startsWith("/")) {
        throw new TypeError("Invalid path: only absolute path is accepted.");
    }
    const encoded = path.split("/").filter(part => part !== "").map(encodeURIComponent).join("/");
    return `${base.replace(/\/+$/, "")}/${encoded}`;
}
