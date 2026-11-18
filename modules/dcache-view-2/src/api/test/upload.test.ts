import { describe, test, expect, vi, beforeEach } from 'vitest';
import { uploadFile } from '../upload.ts';

/** A fake XMLHttpRequest: remembers what is sent, and answers with 201 Created. */
class FakeXhr {
    static last: FakeXhr;
    method = '';
    url = '';
    headers: Record<string, string> = {};
    body: unknown;
    status = 0;
    withCredentials = false;
    upload = { addEventListener: vi.fn() };
    onload: (() => void) | null = null;
    onerror: (() => void) | null = null;

    constructor() {
        FakeXhr.last = this;
    }
    open(method: string, url: string) {
        this.method = method;
        this.url = url;
    }
    setRequestHeader(name: string, value: string) {
        this.headers[name] = value;
    }
    send(body: unknown) {
        this.body = body;
        this.status = 201;
        this.onload?.();
    }
}

/** uploadFile: PUT of the file to WebDAV, password header only when set. */
describe('uploadFile', () => {
    const url = 'https://example.com/some/path/file';
    const file = new File(['hello'], 'file', { type: 'text/plain' });

    beforeEach(() => {
        vi.stubGlobal('XMLHttpRequest', FakeXhr);
    });

    test('sends the file with a PUT, no password header without auth', async () => {
        await uploadFile(url, file);

        expect(FakeXhr.last.method).toBe('PUT');
        expect(FakeXhr.last.url).toBe(url);
        expect(FakeXhr.last.body).toBe(file);
        expect(FakeXhr.last.headers.Authorization).toBeUndefined();
    });

    test('sends the password header with password', async () => {
        await uploadFile(url, file, 'Basic abc');

        expect(FakeXhr.last.headers.Authorization).toBe('Basic abc');
    });
});
