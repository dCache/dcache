// Dummy placeholder module

// Typescript type describing what a DLRequest should contain
interface DownloadRequest {
    url: string;
    filename: string;
}

// Typescript type describing what a DLProgress message should contain
interface DownloadProgress {
    type: "progress" | "done" | "error";
    loaded?: number;
    total?: number;
    error?: string;
}

export function handleMessage(data: DownloadRequest): DownloadProgress {
    // dummy download function returning success message every time
    console.log(`download: ${data.filename} from ${data.url}`);
    return { type: "done" };
}

// module should do when message arrives at worker
self.onmessage = (e: MessageEvent<DownloadRequest>) => {
    self.postMessage(handleMessage(e.data));
};
