export const endpoint: string = "./api/v1/"

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
