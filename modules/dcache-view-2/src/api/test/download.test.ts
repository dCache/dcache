import { describe, test, expect, vi, beforeEach } from 'vitest';
import { downloadFile } from '../download.ts';

/** Builds a fake fetch Response with the given ok flag, JSON body and status. */
function makeResponse(ok: boolean, body: unknown, status = 200): Response {
    return {
        ok,
        status,
        json: () => Promise.resolve(body)
    } as unknown as Response;
}

/** Replaces the link click, so nothing is really downloaded. */
function mockClick() {
    return vi.spyOn(HTMLAnchorElement.prototype, 'click').mockImplementation(() => {});
}

/** downloadFile: plain link without auth, link with macaroon for password, nothing on error. */
describe('downloadFile', () => {
    const url = 'https://example.com/some/path/file';
    const urlWithMacaroon = `${url}?authz=fake-macaroon`;

    beforeEach(() => {
        vi.restoreAllMocks();
    });

    test('without auth links to the file, no macaroon request', async () => {
        const click = mockClick();
        const fetchMock = vi.fn();
        vi.stubGlobal('fetch', fetchMock);

        await downloadFile(url);

        expect(fetchMock).not.toHaveBeenCalled();
        expect((click.mock.contexts[0] as HTMLAnchorElement).href).toBe(url);
    });

    test('with password requests a macaroon and links to the file with it', async () => {
        const click = mockClick();
        const fetchMock = vi.fn().mockResolvedValue(
            makeResponse(true, { uri: { targetWithMacaroon: urlWithMacaroon } }));
        vi.stubGlobal('fetch', fetchMock);

        await downloadFile(url, 'Basic abc');

        expect(fetchMock.mock.calls[0][0]).toBe(url);
        expect((click.mock.contexts[0] as HTMLAnchorElement).href).toBe(urlWithMacaroon);
    });

    test('downloads nothing when the macaroon request fails', async () => {
        const click = mockClick();
        vi.stubGlobal('fetch', vi.fn().mockResolvedValue(makeResponse(false, {}, 401)));

        await expect(downloadFile(url, 'Basic abc')).rejects.toThrow();
        expect(click).not.toHaveBeenCalled();
    });
});
