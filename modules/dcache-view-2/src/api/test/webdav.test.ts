import { describe, test, expect } from 'vitest';
import { webdavEndpoint } from '../webdav.ts';
import type { ViewConfig } from '../../types/types.ts';

/** Builds a minimal config.js content, optionally with a WebDAV endpoint. */
function makeConfig(webdav?: string): ViewConfig {
    return {
        'dcache-view.endpoints.webapi': '/api/v1/',
        'dcache-view.endpoints.webdav': webdav,
        'dcache-view.oidc-provider-name-list': '',
        'dcache-view.oidc-client-id-list': '',
        'dcache-view.oidc-authz-endpoint-list': '',
        'dcache-view.oidc-callback-path': 'auth/callback'
    };
}

/** webdavEndpoint: configured value or <same host>:2880 fallback. */
describe('webdavEndpoint', () => {
    const location = { protocol: 'https:', hostname: 'example.com' } as Location;

    test('uses dcache-view.endpoints.webdav when set', () => {
        expect(webdavEndpoint(makeConfig('https://example.com/'), location)).toBe('https://example.com/');
    });

    test('falls back to same host on port 2880 when not set', () => {
        expect(webdavEndpoint(makeConfig(''), location)).toBe('https://example.com:2880');
        expect(webdavEndpoint(makeConfig(undefined), location)).toBe('https://example.com:2880');
    });
});
