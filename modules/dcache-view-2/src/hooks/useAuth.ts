import { useState, useEffect } from 'react';
import type { User } from '../workers/types.ts';

export function useAuth(): { user: User | null, error: Error | null } {
    const [user, setUser] = useState<User | null>(null);
    const [error, setError] = useState<Error |null>(null);

    useEffect(() => {
        const worker = new Worker(
            new URL('../workers/user-authentification.ts', import.meta.url),
            {type: 'module'}
        );

        worker.onmessage = (e: MessageEvent<User>) => {
            setUser(e.data);
            worker.terminate();
        };

        worker.onerror = (e) => {
            console.warn(e);
            setError(new Error(e.message))
            worker.terminate();
        };
        return () => worker.terminate();
    }, []);

    return { user, error };
}