import type { User } from './types.ts'

export async function getUser(response: Response): Promise<User> {
    if (response.status !== 200) {
        throw new Error(`Looks like there was a problem. Status Code: ${response.status}`);
    }
    return response.json();
}

export function sendUser(user: User) {
    if (user.status === "AUTHENTICATED") {
        self.postMessage(user);
    }
}

const init: RequestInit = {
    credentials: 'include',
    headers: {
        "Suppress-WWW-Authenticate": "Suppress",
        "Accept": "Application/json"
    }
}

export function authenticate() {
    fetch('/api/v1/user', init)
        .then(response => getUser(response))
        .then(user => sendUser(user));
}

authenticate()