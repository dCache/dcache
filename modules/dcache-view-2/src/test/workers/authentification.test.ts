import { describe, test, expect, vi, beforeEach } from 'vitest';
import { getUser, sendUser } from '../../workers/authentification';
import type { User } from '../../workers/types';

const authenticatedUser: User = {
    status: 'AUTHENTICATED',
    uid: 1000,
    gids: [1000, 2000],
    roles: null,
    unassertedRoles: null,
    home: '/home/user',
    root: '/',
    username: 'testuser',
    email: ['test@example.com']
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
        status,
        ok: status >= 200 && status < 300,
        json: () => Promise.resolve(body)
    } as unknown as Response;
}

describe('getUser', () => {
    beforeEach(() => {
        vi.spyOn(console, 'log').mockImplementation(() => {});
    });

   test('returns user on 200 response', async () => {
        const response = makeResponse(200, authenticatedUser);
        const user = await getUser(response);
        expect(user).toEqual(authenticatedUser);
    });

   test('throws on non-200 response', async () => {
        const response = makeResponse(401, anonymousUser);
        await expect(getUser(response)).rejects.toThrow('401');
    });

   test('throws on 500 response', async () => {
        const response = makeResponse(500, anonymousUser);
        await expect(getUser(response)).rejects.toThrow('500');
    });
});

describe('sendUser', () => {
    beforeEach(() => {
        vi.spyOn(console, 'log').mockImplementation(() => {});
        vi.stubGlobal('self', { postMessage: vi.fn() });
    });

   test('posts message when user is AUTHENTICATED', () => {
        sendUser(authenticatedUser);
        expect(self.postMessage).toHaveBeenCalledWith(authenticatedUser);
    });

   test('does not post message when user is ANONYMOUS', () => {
        sendUser(anonymousUser);
        expect(self.postMessage).not.toHaveBeenCalled();
    });
});
