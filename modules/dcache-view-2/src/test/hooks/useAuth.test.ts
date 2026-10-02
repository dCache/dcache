import { test, expect, vi } from 'vitest';
import { renderHook, waitFor } from '@testing-library/react';
import { useAuth } from '../../hooks/useAuth';
import type { User } from "../../workers/types.ts";

const authenticatedUser: User = {
    status: "AUTHENTICATED",
    uid: null,
    gids: null,
    roles: null,
    unassertedRoles: null,
    home: null,
    root: null,
    username: null,
    email: null,
}

vi.stubGlobal('Worker', class {
    onmessage: ((e: MessageEvent) => void) | null = null;
    constructor() {
        setTimeout(() => {
        this.onmessage?.({ data: authenticatedUser } as MessageEvent);
        }, 0);
    }
    terminate() {}
});

test('returns user when authenticated', async () => {
    const { result } = renderHook(() => useAuth());

    expect(result.current?.user).toBeNull(); // initial state

    await waitFor(() => {
        expect(result.current?.user?.status).toBe('AUTHENTICATED');
    });
});
