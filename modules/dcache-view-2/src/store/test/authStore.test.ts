import { beforeEach, describe, expect, test, vi } from 'vitest';
import { useAuthStore, loadCredential } from '../authStore.ts';
import type { User } from '../../types/types.ts';

const UPAUTH_KEY = 'upauth';
const UPAUTH_EXPIRY_KEY = 'upauth_expiry';

const fetchMock = vi.fn();
vi.stubGlobal('fetch', fetchMock);

const authenticatedUser: User = {
    status: 'AUTHENTICATED',
    uid: 1000,
    gids: [1000],
    roles: null,
    unassertedRoles: null,
    home: '/home/user',
    root: '/',
    username: 'testuser',
    email: null
};

const anonymousUser: User = {
    status: 'ANONYMOUS',
    uid: null,
    gids: null,
    roles: null,
    unassertedRoles: null,
    home: null,
    root: null,
    username: null,
    email: null
};

function makeResponse(status: number, body: User): Response {
    return {
        status: status,
        ok: status >= 200 && status < 300,
        json: () => Promise.resolve(body)
    } as unknown as Response;
}

beforeEach(() => {
    localStorage.clear();
    fetchMock.mockReset();
    useAuthStore.setState({ user: null, upauth: null, error: null });
});

describe('loadCredential', () => {
    test('returns null and clears localStorage when expired', () => {
        localStorage.setItem(UPAUTH_KEY, 'dXNlcjpwYXNz');
        localStorage.setItem(UPAUTH_EXPIRY_KEY, String(Date.now() - 1));

        expect(loadCredential()).toBeNull();
        expect(localStorage.getItem(UPAUTH_KEY)).toBeNull();
        expect(localStorage.getItem(UPAUTH_EXPIRY_KEY)).toBeNull();
    });

    test('returns credential when not expired', () => {
        localStorage.setItem(UPAUTH_KEY, 'dXNlcjpwYXNz');
        localStorage.setItem(UPAUTH_EXPIRY_KEY, String(Date.now() + 60000));

        expect(loadCredential()).toBe('dXNlcjpwYXNz');
    });

});

describe('passiveAuth', () => {
    test('restores session from valid stored credential', async () => {
        localStorage.setItem(UPAUTH_KEY, 'dXNlcjpwYXNz');
        localStorage.setItem(UPAUTH_EXPIRY_KEY, String(Date.now() + 60000));
        fetchMock.mockResolvedValue(makeResponse(200, authenticatedUser));

        await useAuthStore.getState().passiveAuth();

        const state = useAuthStore.getState();
        expect(state.user).toEqual(authenticatedUser);
        expect(state.upauth).toBe('dXNlcjpwYXNz');
    });

    test('clears stored credential on rejection and leaves user null', async () => {
        localStorage.setItem(UPAUTH_KEY, 'dXNlcjpwYXNz');
        localStorage.setItem(UPAUTH_EXPIRY_KEY, String(Date.now() + 60000));
        fetchMock.mockResolvedValue(makeResponse(401, anonymousUser));

        await useAuthStore.getState().passiveAuth();

        expect(localStorage.getItem(UPAUTH_KEY)).toBeNull();
        expect(useAuthStore.getState().user).toBeNull();
    });

    test('clears stored credential when server returns ANONYMOUS for it', async () => {
        localStorage.setItem(UPAUTH_KEY, 'dXNlcjpwYXNz');
        localStorage.setItem(UPAUTH_EXPIRY_KEY, String(Date.now() + 60000));
        fetchMock.mockResolvedValue(makeResponse(200, anonymousUser));

        await useAuthStore.getState().passiveAuth();

        expect(localStorage.getItem(UPAUTH_KEY)).toBeNull();
        expect(useAuthStore.getState().user).toBeNull();
    });

    test('sets anonymous user when no session exists', async () => {
        fetchMock.mockResolvedValue(makeResponse(200, anonymousUser));

        await useAuthStore.getState().passiveAuth();

        const state = useAuthStore.getState();
        expect(state.user).toEqual(anonymousUser);
        expect(state.upauth).toBe(btoa('anonymous:nopassword'));
    });

    test('sets authenticated user from cookie or certificate', async () => {
        fetchMock.mockResolvedValue(makeResponse(200, authenticatedUser));

        await useAuthStore.getState().passiveAuth();

        const state = useAuthStore.getState();
        expect(state.user).toEqual(authenticatedUser);
        expect(state.upauth).toBeNull();
    });
});

describe('login', () => {
    test('sets user and stores credential on success', async () => {
        fetchMock.mockResolvedValue(makeResponse(200, authenticatedUser));

        await useAuthStore.getState().login('testuser', 'password');

        const state = useAuthStore.getState();
        expect(state.user).toEqual(authenticatedUser);
        expect(state.upauth).toBe(btoa('testuser:password'));
        expect(localStorage.getItem(UPAUTH_KEY)).toBe(btoa('testuser:password'));
    });

    test('throws when server returns non-authenticated status', async () => {
        fetchMock.mockResolvedValue(makeResponse(200, anonymousUser));

        await expect(useAuthStore.getState().login('user', 'wrong')).rejects.toThrow('Login failed');
    });

    test('throws when server returns error response', async () => {
        fetchMock.mockResolvedValue(makeResponse(401, anonymousUser));

        await expect(useAuthStore.getState().login('user', 'pass')).rejects.toThrow('401');
    });
});

describe('logout', () => {
    test('clears user, upauth, and stored credential', () => {
        useAuthStore.setState({ user: authenticatedUser, upauth: 'dXNlcjpwYXNz', error: null });
        localStorage.setItem(UPAUTH_KEY, 'dXNlcjpwYXNz');
        localStorage.setItem(UPAUTH_EXPIRY_KEY, String(Date.now() + 60000));

        useAuthStore.getState().logout();

        const state = useAuthStore.getState();
        expect(state.user).toBeNull();
        expect(state.upauth).toBeNull();
        expect(localStorage.getItem(UPAUTH_KEY)).toBeNull();
        expect(localStorage.getItem(UPAUTH_EXPIRY_KEY)).toBeNull();
    });
});
