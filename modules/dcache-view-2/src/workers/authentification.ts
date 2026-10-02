import type { User } from './types.ts'

function getUser(response: Response): Promise<User> {
    console.log("IS USER RESOURCE STILL CALLED ");
    if (response.status !== 200) {
        throw new Error(`Looks like there was a problem. Status Code: ${response.status}`);
    }
    return response.json();
}

function sendUser(user: User) {
    console.log("AUTHENTICATED user resource: ");
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

fetch('/api/v1/user', init)
    .then(response => getUser(response))
    .then(user => sendUser(user));