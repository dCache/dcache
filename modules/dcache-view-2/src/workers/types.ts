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

export interface FileContentRequest {
    url: string;
    mime: string;
    upauth?: string;
}

export interface FileContent {
    data : Blob | unknown;
}

export interface FileContentResponse {
    data: unknown;
    loading: boolean;
    error: Error | null;
}