import type {IdP, ViewConfig} from "../types/types.ts";

async function fetchProviders(): Promise<ViewConfig> {
    const headers: Record<string, string> = {
        'Suppress-WWW-Authenticate': 'Suppress',
        'Accept': 'application/json',
    };
    const response: Response = await fetch('/scripts/config.js',
        { credentials: 'include', headers } as RequestInit);
    if (response.status !== 200) throw new Error(`${response.status}`);
    return await response.json() as Promise<ViewConfig>;
}

function splitList(value?: string): string[] {
    return (value ?? "").trim().split(/\s+/).filter(item => item !== "");
}

function extractProviders (conf: ViewConfig): IdP[] {
    const names: string[] = splitList(conf["dcache-view.oidc-provider-name-list"]);
    const clientIds: string[] = splitList(conf["dcache-view.oidc-client-id-list"]);
    const authEndPoints: string[] = splitList(conf["dcache-view.oidc-authz-endpoint-list"]);
    const authEndPointExtras: string[] = splitList(conf["dcache-view.oidc-authz-endpoint-extra"]);
    const callBackPath: string = conf["dcache-view.endpoints.webapi"] + conf["dcache-view.oidc-callback-path"].trim();

    const idps: IdP[] = names.map((name, i) => ({
        name: name,
        clientId: clientIds[i],
        authEndPoint: authEndPoints[i],
        authEndPointExtra: authEndPointExtras[i] === "-" ? undefined : authEndPointExtras[i],
        callBackPath: callBackPath
    }))
    return idps;
}

export function getOIDCProviders() {
    return fetchProviders().then(c => extractProviders(c));
}

export function authURL(origin: string, idp: IdP): string {
    const url: URL = new URL(idp.authEndPoint);
    url.searchParams.set("response_type", "code");
    url.searchParams.set("client_id", idp.clientId);
    url.searchParams.set("redirect_uri", origin + idp.callBackPath);
    url.searchParams.set("scope", "openid profile email");
    url.searchParams.set("state", idp.clientId);
    if (idp.authEndPointExtra) {
        new URLSearchParams(idp.authEndPointExtra).forEach((value, key) => url.searchParams.append(key, value));
    }
    return url.toString();
}