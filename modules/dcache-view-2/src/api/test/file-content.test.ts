import { describe, test, expect, vi, beforeEach } from 'vitest';
import { processResponse } from '../file-content.ts';
import type { FileContentRequest } from '../../types/types.ts';

const blobRequest: FileContentRequest = { url: 'https://example.com/file', mime: 'application/byte-stream' };
const jsonRequest: FileContentRequest = { url: 'https://example.com/file', mime: 'application/json' };

describe('processResponse', () => {
    beforeEach(() => {
        vi.spyOn(console, 'log').mockImplementation(() => {});
    });

    test('returns blob data for non-json mime type', async () => {
        const blob = new Blob(['hello']);
        const response = { ok: true, status: 200, url: 'https://example.com/file', blob: () => Promise.resolve(blob), json: vi.fn() } as unknown as Response;
        const result = await processResponse(response, blobRequest);
        expect(result?.data).toBe(blob);
    });

    test('returns json data for json mime type', async () => {
        const json = { foo: 'bar' };
        const response = { ok: true, status: 200, url: 'https://example.com/file', json: () => Promise.resolve(json), blob: vi.fn() } as unknown as Response;
        const result = await processResponse(response, jsonRequest);
        expect(result?.data).toEqual(json);
    });

    test('throws with client error message on 4xx response', async () => {
        const response = { ok: false, status: 404, url: 'https://example.com/file' } as Response;
        await expect(processResponse(response, blobRequest)).rejects.toThrow('Request failed with response status code 404.');
    });

    test('throws with server error message on 5xx response', async () => {
        const response = { ok: false, status: 500, url: 'https://example.com/file' } as Response;
        await expect(processResponse(response, blobRequest)).rejects.toThrow('dCache Internal Server Error');
    });

    test('throws on unexpected status code', async () => {
        const response = { ok: false, status: 301, url: 'https://example.com/file' } as Response;
        await expect(processResponse(response, blobRequest)).rejects.toThrow('Unexpected response status code 301.');
    });
});
