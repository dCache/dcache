export interface User {
    status: 'ANONYMOUS' | 'AUTHENTICATED';
    uid: number | null;
    gids: number[] | null;
    roles: string[] | null;
    unassertedRoles: string[] | null;
    home: string | null;
    root: string | null;
    username: string | null;
    email: string[] | null;
}

export interface WorkerMessageData {
    url: string;
    mime: string;
    upauth?: string;
    return: 'json' | 'blob';
}

export interface WorkerResponse {
    data : Blob | object;
}