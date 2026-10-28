import type { LabelRequest, LabelAction, FileAttributes } from "../types/types.ts";
// TODO: move endpoint and buildHeaders to a shared src/api/util.ts (also used in namespace.ts)
import { buildHeaders } from "./namespace.ts";

const endpoint: string = "./api/v1/"

/** Adds a label to a file. Throws on error (see throwOnError). */
export async function setLabel(r: LabelRequest): Promise<void> {
    return postLabelAction(r, "set-label");
}

/** Removes a label from a file. Throws on error (see throwOnError). */
export async function removeLabel(r: LabelRequest): Promise<void> {
    return postLabelAction(r, "rm-label");
}

/** POST /api/v1/namespace{path} with {action, label}. r.upauth is the full header value ("Basic ..."). */
async function postLabelAction(r: LabelRequest, action: LabelAction): Promise<void> {
    const response = await fetch(new Request(`${endpoint}namespace${r.path}`, {
        method: "POST",
        headers: buildHeaders(r.upauth),
        credentials: "include",
        body: JSON.stringify({ action: action, label: r.label })
    }));
    await throwOnError(response);
}

/** Returns the labels of a file, or [] if it has none. Throws on error (see throwOnError). */
export async function getFileLabels(r: Pick<LabelRequest, 'path' | 'upauth'>): Promise<string[]> {
    const response = await fetch(new Request(`${endpoint}namespace${r.path}?labels=true`, {
        headers: buildHeaders(r.upauth),
        credentials: "include"
    }));
    await throwOnError(response);
    const attributes = await response.json() as FileAttributes;
    return attributes.labels ?? [];
}

/**
 * Throws "status: statusText - detail" if the response is not ok.
 * detail comes from the dCache JSON error body, if present and different from statusText.
 */
async function throwOnError(response: Response): Promise<void> {
    if (!response.ok) {
        const body = await response.json().catch(() => null);
        const detail = body?.detail && body.detail !== response.statusText ? ` - ${body.detail}` : "";
        throw new Error(`${response.status}: ${response.statusText}${detail}`);
    }
}
