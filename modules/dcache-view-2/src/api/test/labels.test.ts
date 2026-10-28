import { describe, test, expect, vi, beforeEach } from 'vitest';
import { setLabel, removeLabel, getFileLabels } from '../labels.ts';
import type { LabelRequest } from '../../types/types.ts';

/** Builds a fake fetch Response with the given ok flag, JSON body, status and statusText. */
function makeResponse(ok: boolean, body: unknown, status = 201, statusText = 'Created'): Response {
    return {
        ok,
        status,
        statusText,
        json: () => Promise.resolve(body)
    } as unknown as Response;
}

/** setLabel: request format, Authorization header and error without detail. */
describe('setLabel', () => {
    const r: LabelRequest = { path: '/some/path/file', label: 'run2026', upauth: 'Basic dXNlcjpwYXNz' };

    beforeEach(() => {
        vi.restoreAllMocks();
    });

    test('sends POST to namespace path with set-label body', async () => {
        const fetchMock = vi.fn().mockResolvedValue(makeResponse(true, { status: 'success' }));
        vi.stubGlobal('fetch', fetchMock);

        await setLabel(r);

        expect(fetchMock).toHaveBeenCalledTimes(1);
        const request: Request = fetchMock.mock.calls[0][0];
        expect(request.method).toBe('POST');
        expect(request.url).toContain('namespace/some/path/file');
        expect(await request.json()).toEqual({ action: 'set-label', label: 'run2026' });
    });

    test('sends Authorization header when upauth is set', async () => {
        const fetchMock = vi.fn().mockResolvedValue(makeResponse(true, { status: 'success' }));
        vi.stubGlobal('fetch', fetchMock);

        await setLabel(r);

        const request: Request = fetchMock.mock.calls[0][0];
        expect(request.headers.get('Authorization')).toBe('Basic dXNlcjpwYXNz');
    });

    test('throws on non-ok response', async () => {
        vi.stubGlobal('fetch', vi.fn().mockResolvedValue(makeResponse(false, null, 404, 'Not Found')));

        await expect(setLabel(r)).rejects.toThrow('404: Not Found');
    });
});

/** removeLabel: request format. */
describe('removeLabel', () => {
    const r: LabelRequest = { path: '/some/path/file', label: 'run2026', upauth: 'Basic dXNlcjpwYXNz' };

    beforeEach(() => {
        vi.restoreAllMocks();
    });

    test('sends POST to namespace path with rm-label body', async () => {
        const fetchMock = vi.fn().mockResolvedValue(makeResponse(true, { status: 'success' }));
        vi.stubGlobal('fetch', fetchMock);

        await removeLabel(r);

        expect(fetchMock).toHaveBeenCalledTimes(1);
        const request: Request = fetchMock.mock.calls[0][0];
        expect(request.method).toBe('POST');
        expect(request.url).toContain('namespace/some/path/file');
        expect(await request.json()).toEqual({ action: 'rm-label', label: 'run2026' });
    });
});

/** getFileLabels: GET with labels=true, returns [] when the file has no labels. */
describe('getFileLabels', () => {
    beforeEach(() => {
        vi.restoreAllMocks();
    });

    test('sends GET with labels=true and returns labels', async () => {
        const fetchMock = vi.fn().mockResolvedValue(makeResponse(true, { fileType: 'REGULAR', labels: ['a', 'b'] }, 200, 'OK'));
        vi.stubGlobal('fetch', fetchMock);

        await expect(getFileLabels({ path: '/some/path/file' })).resolves.toEqual(['a', 'b']);

        const request: Request = fetchMock.mock.calls[0][0];
        expect(request.method).toBe('GET');
        expect(request.url).toContain('namespace/some/path/file?labels=true');
    });

    test('returns empty array when labels are missing', async () => {
        vi.stubGlobal('fetch', vi.fn().mockResolvedValue(makeResponse(true, { fileType: 'REGULAR', labels: null }, 200, 'OK')));

        await expect(getFileLabels({ path: '/some/path/file' })).resolves.toEqual([]);
    });
});
