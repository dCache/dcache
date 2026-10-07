import { useState, useEffect } from 'react';
import type {FileContent, FileContentResponse} from "../workers/types.ts";

export function useFileContent(url: string, mime: string, upauth?: string): FileContentResponse {
    const [data, setData] = useState<Blob | unknown | null>(null);
    const [loading, setLoading] = useState<boolean>(true);
    const [error, setError] = useState<Error | null>(null);

    useEffect(() => {
        setLoading(true);
        const worker = new Worker(
            new URL('..workers/file-content.ts', import.meta.url),
            {type: 'module'}
        );

        worker.postMessage({url, mime, upauth})

        worker.onmessage = (e: MessageEvent<FileContent>) => {
            setData(e.data);
            setLoading(false);
            worker.terminate();
        };

        worker.onerror = (e: ErrorEvent)=>{
            console.warn(e);
            setError(new Error(e.message));
            setLoading(false);
            worker.terminate();
        };
        return () => worker.terminate();
    }, []);

    return {data, loading, error} as FileContentResponse;
}