import {describe, test, expect, vi, beforeEach} from 'vitest';
import { buildRequest, processResponse } from '../../workers/file-content.ts';
import type { FileContentRequest } from '../../workers/types.ts';

function makeEvent(data: FileContentRequest): MessageEvent<FileContentRequest> {
    return { data } as MessageEvent<FileContentRequest>;
}

const withAuth: FileContentRequest = { url: 'https://example.com/file', mime: 'application/octet-stream', upauth: 'token161' };
const noAuth: FileContentRequest = { url: 'https://example.com/file', mime: 'application/octet-stream', upauth: '' };
const byte: FileContentRequest = { url: 'https://example.com/file', mime: 'application/byte-stream'}
const JSON: FileContentRequest = { url: 'https://example.com/file', mime: 'application/json'}

describe('request', () => {
    test('adds Authorization header when upauth is provided', () => {
        const e = makeEvent(withAuth);
        const request = buildRequest(e);
        expect(request.headers.get('Authorization')).toBe('token161');
    });

    test('omits Authorization header when upauth is empty', () => {
        const e = makeEvent(noAuth);
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
        const e = makeEvent(byte);
        const result = await processResponse(response, e);
        expect(result?.data).toBe(blob);
    });

    test('returns json data on ok response with return=json', async () => {
        const json = { foo: 'bar' };
        const response = { ok: true, status: 200, url: 'https://example.com/file', json: () => Promise.resolve(json), blob: vi.fn() } as unknown as Response;
        const e = makeEvent(JSON);
        const result = await processResponse(response, e);
        expect(result?.data).toEqual(json);
    });

    test('throws on 4xx response', async () => {
        const response = { ok: false, status: 404, url: 'https://example.com/file' } as Response;
        const e = makeEvent(byte);
        await expect(processResponse(response, e)).rejects.toThrow('404');
    });

    test('throws on 5xx response', async () => {
        const response = { ok: false, status: 500, url: 'https://example.com/file' } as Response;
        const e = makeEvent(byte);
        await expect(processResponse(response, e)).rejects.toThrow('500');
    });
});
