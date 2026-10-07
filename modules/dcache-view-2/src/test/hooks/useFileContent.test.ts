import { test, expect, vi, describe, beforeEach } from 'vitest';
import { renderHook, waitFor } from '@testing-library/react';
import { useFileContent } from '../../hooks/useFileContent';
import type { FileContent } from '../../workers/types';

const blobData = new Blob(['file content'], { type: 'text/plain' });
const JSON = { key: 'value' };

function makeWorker(response: FileContent | null, error: ErrorEvent | null = null) {
    return class {
        onmessage: ((e: MessageEvent<FileContent>) => void) | null = null;
        onerror: ((e: ErrorEvent) => void) | null = null;

        postMessage() {
            setTimeout(() => {
                if (error) {
                    this.onerror?.(error);
                } else {
                    this.onmessage?.({ data: response } as MessageEvent<FileContent>);
                }
            }, 0);
        }
        terminate() {}
    };
}

describe('useFileContent', () => {
    beforeEach(() => {
        vi.restoreAllMocks();
    });

    test('starts with loading true and no data', () => {
        vi.stubGlobal('Worker', makeWorker({ data: blobData }));
        const { result } = renderHook(() => useFileContent('/path/file.txt', 'text/plain'));
        expect(result.current.loading).toBe(true);
        expect(result.current.data).toBeNull();
        expect(result.current.error).toBeNull();
    });

    test('returns blob for non-json mime type', async () => {
        vi.stubGlobal('Worker', makeWorker({ data: blobData }));
        const { result } = renderHook(() => useFileContent('/path/file.txt', 'text/plain'));

        await waitFor(() => {
            expect(result.current.loading).toBe(false);
        });

        expect(result.current.data).toEqual({ data: blobData });
        expect(result.current.error).toBeNull();
    });

    test('returns json for application/json mime type', async () => {
        vi.stubGlobal('Worker', makeWorker({ data: JSON }));
        const { result } = renderHook(() => useFileContent('/path/file.json', 'application/json'));

        await waitFor(() => {
            expect(result.current.loading).toBe(false);
        });

        expect(result.current.data).toEqual({ data: JSON });
        expect(result.current.error).toBeNull();
    });

    test('sets error and stops loading on worker error', async () => {
        const errorEvent = new ErrorEvent('error', { message: 'fetch failed' });
        vi.stubGlobal('Worker', makeWorker(null, errorEvent));
        vi.spyOn(console, 'warn').mockImplementation(() => {});

        const { result } = renderHook(() => useFileContent('/path/file.txt', 'text/plain'));

        await waitFor(() => {
            expect(result.current.loading).toBe(false);
        });

        expect(result.current.error?.message).toBe('fetch failed');
        expect(result.current.data).toBeNull();
    });
});
