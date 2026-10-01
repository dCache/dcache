import {describe, test, expect, vi, beforeEach} from 'vitest';
import { buildRequest, processResponse } from '../../workers/download.ts';
import type { WorkerMessageData } from '../../workers/types.ts';

function makeEvent(data: WorkerMessageData): MessageEvent<WorkerMessageData> {
    return { data } as MessageEvent<WorkerMessageData>;
}

describe('buildRequest', () => {
    test('adds Authorization header when upauth is provided', () => {
        const e = makeEvent({ url: 'https://example.com/file', mime: 'application/octet-stream', upauth: 'token161', return: 'blob' });
        const request = buildRequest(e);
        expect(request.headers.get('Authorization')).toBe('token161');
    });

    test('omits Authorization header when upauth is empty', () => {
        const e = makeEvent({ url: 'https://example.com/file', mime: 'application/octet-stream', upauth: '', return: 'blob' });
        const request = buildRequest(e);
        expect(request.headers.get('Authorization')).toBeNull();
    });
});

describe('processResponse', () => {
    beforeEach(() => {
        vi.spyOn(console, 'log').mockImplementation(() => {});
    });

    test('returns blob data on ok response with return=blob', async () => {
        const blob = new Blob(['hello']);
        const response = { ok: true, status: 200, url: 'https://example.com/file', blob: () => Promise.resolve(blob), json: vi.fn() } as unknown as Response;
        const e = makeEvent({ url: 'https://example.com/file', mime: 'application/octet-stream', return: 'blob' });
        const result = await processResponse(response, e);
        expect(result?.data).toBe(blob);
    });

    test('returns json data on ok response with return=json', async () => {
        const json = { foo: 'bar' };
        const response = { ok: true, status: 200, url: 'https://example.com/file', json: () => Promise.resolve(json), blob: vi.fn() } as unknown as Response;
        const e = makeEvent({ url: 'https://example.com/file', mime: 'application/json', return: 'json' });
        const result = await processResponse(response, e);
        expect(result?.data).toEqual(json);
    });

    test('throws on 4xx response', async () => {
        const response = { ok: false, status: 404, url: 'https://example.com/file' } as Response;
        const e = makeEvent({ url: 'https://example.com/file', mime: 'application/octet-stream', return: 'blob' });
        await expect(processResponse(response, e)).rejects.toThrow('404');
    });

    test('throws on 5xx response', async () => {
        const response = { ok: false, status: 500, url: 'https://example.com/file' } as Response;
        const e = makeEvent({ url: 'https://example.com/file', mime: 'application/octet-stream', return: 'blob' });
        await expect(processResponse(response, e)).rejects.toThrow('500');
    });
});
