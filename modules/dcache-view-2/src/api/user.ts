import type { User } from '../workers/types.ts'

export async function getUser(init: RequestInit): Promise<User> {
    const response: Response = await fetch('api/v1/user', init);
    if (response.status !== 200) throw new Error(`${response.status}`);
    return response.json();
}