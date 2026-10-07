import { useState, useEffect } from 'react'
import type {FileAttributes, FileMetadataResponse, FileMetadataRequest} from "../workers/types.ts";

export function useMetadata(request: FileMetadataRequest): FileMetadataResponse{
    const [data, setData] = useState<FileAttributes | null>(null);
    const [error, setError] = useState<Error | null>(null);

    useEffect(() => {
        const worker = new Worker(
            new URL('../workers/file-metadata.ts', import.meta.url),
            {type: 'module'}
        );

        worker.postMessage(request)

        worker.onmessage = (e: MessageEvent<FileAttributes>) => {
            setData(e.data)
            worker.terminate();
        };

        worker.onerror = (e: ErrorEvent)=>{
            console.warn(e);
            setError(new Error(e.message));
            worker.terminate();
        };
        return () => worker.terminate();
    }, []);
    return {data, error} as FileMetadataResponse;
}
