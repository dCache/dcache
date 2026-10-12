import { create } from 'zustand';
import {getUser} from "../api/user.ts";
import type { User } from '../workers/types.ts';

const UPAUTH_KEY: string = 'upauth';
const UPAUTH_EXPIRY_KEY: string = 'upauth_expiry';
const SESSION_DURATION_MS: number = 600000; // 10 minutes

interface AuthState {
    user: User | null;
    upauth: string | null;
    error: Error | null;
    passiveAuth: () => void;
    login: (username: string, password: string) => Promise<void>;
    logout: () => void;
}

async function fetchUser(upauth?: string): Promise<User> {
    const headers: Record<string, string> = {
        'Suppress-WWW-Authenticate': 'Suppress',
        'Accept': 'application/json',
    };
    if (upauth) headers['Authorization'] = `Basic ${upauth}`;
    return getUser({ credentials: 'include', headers } as RequestInit)
}

function storeCredential(upauth: string): void {
    sessionStorage.setItem(UPAUTH_KEY, upauth);
    sessionStorage.setItem(UPAUTH_EXPIRY_KEY, String(Date.now() + SESSION_DURATION_MS));
}

function clearCredential(): void {
    sessionStorage.removeItem(UPAUTH_KEY);
    sessionStorage.removeItem(UPAUTH_EXPIRY_KEY);
}

export function loadCredential(): string | null {
    const upauth: string | null = sessionStorage.getItem(UPAUTH_KEY);
    const expiry: number = Number(sessionStorage.getItem(UPAUTH_EXPIRY_KEY));
    if (!upauth || Date.now() > expiry) {
        clearCredential();
        return null;
    }
    return upauth;
}

export const useAuthStore = create<AuthState>((set) => ({
    user: null,
    upauth: null,
    error: null,

    passiveAuth: async () => {
        const storedUpauth: string | null = loadCredential();

        if (storedUpauth) {
            try {
                const user: User = await fetchUser(storedUpauth);
                if (user.status === 'AUTHENTICATED') {
                    set({ user: user, upauth: storedUpauth });
                    return;
                }
            } catch (e) {
                console.warn('Stored credential rejected:', e);
            }
            clearCredential();
        } else {
            try {
                const user: User = await fetchUser();
                const upauth = user.status === 'ANONYMOUS'
                    ? btoa('anonymous:nopassword')
                    : null;
                set({ user: user, upauth: upauth });
            } catch (e) {
                set({ error: e as Error });
            }
        }
    },

    login: async (username: string, password: string) => {
        const b64: string = btoa(`${username}:${password}`);
        const user: User = await fetchUser(b64);
        if (user.status !== 'AUTHENTICATED') throw new Error('Login failed');
        storeCredential(b64);
        set({ user: user, upauth: b64 });
    },

    logout: () => {
        clearCredential();
        set({ user: null, upauth: null, error: null });
    },
}));
