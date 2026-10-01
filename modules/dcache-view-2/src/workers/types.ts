export interface WorkerMessageData {
    url: string;
    mime: string;
    upauth?: string;
    return: 'json' | 'blob';
}

export interface WorkerResponse {
    data : Blob | object;
}