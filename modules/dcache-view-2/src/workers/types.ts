export interface FileAttributes {
    pnfsId: string;
    fileType: 'REGULAR' | 'DIR' | 'LINK' | 'SPECIAL';
    size: number | null;
    creationTime: number | null;
    modificationTime: number | null;
    accessTime: number | null;
    changeTime: number | null;
    owner: number | null;
    group: number | null;
    mode: number | null;
    accessLatency: 'ONLINE' | 'NEARLINE' | null;
    retentionPolicy: 'CUSTODIAL' | 'OUTPUT' | 'REPLICA' | null;
    checksums: { type: string; value: string }[] | null;
    nlink: number | null;
    storageClass: string | null;
    cacheClass: string | null;
    hsm: string | null;
    xattrs: Record<string, string> | null;
    labels: string[] | null;
    qosPolicy: string | null;
    qosState: number | null;
    locations: string[] | null;
}

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

export interface FileMetadataRequest {
    pnfsId?: string;
    path?: string;
    upauth?: string;
    scope: 'partial' | 'full';
    limit: number | 'max';
    offset: number;
}
