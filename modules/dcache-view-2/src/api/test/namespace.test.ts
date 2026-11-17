import { describe, test, expect, vi, beforeEach } from 'vitest';
import { fileAttributes, buildHeaders, ls } from '../namespace.ts';
import type { FileAttributes, FileMetadataRequest } from '../../types/types.ts';

const mockAttributes: FileAttributes = {
    pnfsId: 'abc123',
    fileType: 'REGULAR',
    size: 1024,
    creationTime: 1000000,
    modificationTime: 2000000,
    accessTime: 3000000,
    changeTime: 4000000,
    owner: 1000,
    group: 1000,
    mode: 644,
    accessLatency: 'ONLINE',
    retentionPolicy: 'REPLICA',
    checksums: null,
    nlink: 1,
    storageClass: null,
    cacheClass: null,
    hsm: null,
    xattrs: null,
    labels: null,
    qosPolicy: null,
    qosState: null,
    locations: null,
};

function makeResponse(ok: boolean, body: unknown, status = 200, statusText = 'OK'): Response {
    return {
        ok,
        status,
        statusText,
        json: () => Promise.resolve(body)
    } as unknown as Response;
}

describe('fileAttributes', () => {
    const headers = buildHeaders();

    beforeEach(() => {
        vi.restoreAllMocks();
    });

    test('fetches by pnfsId when scope is full', async () => {
        const fetchMock = vi.fn().mockResolvedValue(makeResponse(true, mockAttributes));
        vi.stubGlobal('fetch', fetchMock);

        const r: FileMetadataRequest = { pnfsId: 'abc123', scope: 'full', limit: 100, offset: 0 };
        await fileAttributes(r, headers);

        expect(fetchMock).toHaveBeenCalledTimes(1);
        expect(fetchMock.mock.calls[0][0].url).toContain('id/abc123');
    });

    test('fetches by path when scope is partial with correct query parameters', async () => {
        const fetchMock = vi.fn().mockResolvedValue(makeResponse(true, mockAttributes));
        vi.stubGlobal('fetch', fetchMock);

        const r: FileMetadataRequest = { path: '/some/path', scope: 'partial', limit: 100, offset: 5 };
        await fileAttributes(r, headers);

        expect(fetchMock).toHaveBeenCalledTimes(1);
        const url = fetchMock.mock.calls[0][0].url;
        expect(url).toContain('namespace/some/path');
        expect(url).toContain('children=true');
        expect(url).toContain('limit=100');
        expect(url).toContain('offset=5');
        // qos=true makes the backend ask the PinManager for every entry and fails with 500 without one
        expect(url).not.toContain('qos');
    });

    test('fetches partial first then full when scope is full but no pnfsId', async () => {
        const fetchMock = vi.fn().mockResolvedValue(makeResponse(true, mockAttributes));
        vi.stubGlobal('fetch', fetchMock);

        const r: FileMetadataRequest = { path: '/some/path', scope: 'full', limit: 100, offset: 0 };
        await fileAttributes(r, headers);

        expect(fetchMock).toHaveBeenCalledTimes(2);
        expect(fetchMock.mock.calls[0][0].url).toContain('namespace/some/path');
        expect(fetchMock.mock.calls[1][0].url).toContain('id/abc123');
    });

    test('uses max integer for limit max', async () => {
        const fetchMock = vi.fn().mockResolvedValue(makeResponse(true, mockAttributes));
        vi.stubGlobal('fetch', fetchMock);

        const r: FileMetadataRequest = { path: '/some/path', scope: 'partial', limit: 'max', offset: 0 };
        await fileAttributes(r, headers);

        expect(fetchMock.mock.calls[0][0].url).toContain('limit=2147483647');
    });

    test('throws on non-ok response', async () => {
        vi.stubGlobal('fetch', vi.fn().mockResolvedValue(makeResponse(false, null, 404, 'Not Found')));

        const r: FileMetadataRequest = { pnfsId: 'abc123', scope: 'full', limit: 100, offset: 0 };
        await expect(fileAttributes(r, headers)).rejects.toThrow('404: Not Found');
    });

    test('throws TypeError when neither path nor pnfsId provided', async () => {
        const r: FileMetadataRequest = { scope: 'partial', limit: 100, offset: 0 };
        await expect(fileAttributes(r, headers)).rejects.toThrow(TypeError);
    });
});

describe('ls', () => {
    const dir: FileAttributes = { ...mockAttributes, fileType: 'DIR' };

    test('returns the children of a directory', () => {
        const child: FileAttributes = { ...mockAttributes, fileName: 'a.txt' };
        expect(ls({ ...dir, children: [child] })).toEqual([child]);
    });

    test('returns an empty list when the response has no children', () => {
        expect(ls(dir)).toEqual([]);
    });

    test('throws TypeError for a file', () => {
        expect(() => ls(mockAttributes)).toThrow(TypeError);
    });
});
