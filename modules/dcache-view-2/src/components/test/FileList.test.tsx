import { afterEach, beforeEach, describe, expect, test, vi } from 'vitest';
import { cleanup, fireEvent, render, screen } from '@testing-library/react';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { FileList } from '../FileList.tsx';
import { useAuthStore } from '../../store/authStore.ts';
import type { FileAttributes } from '../../types/types.ts';

function entry(fileName: string, fileType: FileAttributes['fileType']): FileAttributes {
    return { pnfsId: `id-${fileName}`, fileName: fileName, fileType: fileType } as FileAttributes;
}

// directory path -> entries returned by the backend
const tree: Record<string, FileAttributes[]> = {
    '/': [entry('a', 'DIR'), entry('readme.txt', 'REGULAR')],
    '/a': [entry('b', 'DIR')],
    '/a/b': [entry('deep.txt', 'REGULAR')],
};

const fetchMock = vi.fn((request: Request) => {
    const path = decodeURIComponent(new URL(request.url).pathname.replace(/^.*\/api\/v1\/namespace/, ''));
    const body = { fileType: 'DIR', pnfsId: `id-${path}`, children: tree[path] };
    return Promise.resolve({ ok: true, json: () => Promise.resolve(body) } as unknown as Response);
});

function requestedPaths(): string[] {
    return fetchMock.mock.calls.map(([request]) =>
        new URL(request.url).pathname.replace(/^.*\/api\/v1\/namespace/, ''));
}

function renderFileList() {
    const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } });
    render(
        <QueryClientProvider client={queryClient}>
            <FileList />
        </QueryClientProvider>
    );
}

beforeEach(() => {
    fetchMock.mockClear();
    vi.stubGlobal('fetch', fetchMock);
    useAuthStore.setState({ upauth: 'dXNlcjpwYXNz' });
});

afterEach(cleanup);

describe('FileList', () => {
    test('starts at root and opens a directory on click', async () => {
        renderFileList();

        fireEvent.click(await screen.findByRole('button', { name: /a$/ }));

        expect(await screen.findByRole('button', { name: /b$/ })).toBeTruthy();
        expect(requestedPaths()).toEqual(['/', '/a']);
    });

    test('files are not clickable', async () => {
        renderFileList();

        expect(await screen.findByText('readme.txt')).toBeTruthy();
        expect(screen.queryByRole('button', { name: /readme\.txt/ })).toBeNull();
    });

    test('back goes to the parent directory and is hidden at root', async () => {
        renderFileList();
        expect(screen.queryByRole('button', { name: /Back/ })).toBeNull();

        fireEvent.click(await screen.findByRole('button', { name: /a$/ }));
        fireEvent.click(await screen.findByRole('button', { name: /b$/ }));
        expect(await screen.findByText('deep.txt')).toBeTruthy();

        fireEvent.click(screen.getByRole('button', { name: /Back/ }));
        expect(await screen.findByRole('button', { name: /b$/ })).toBeTruthy();   // back in /a, not /

        fireEvent.click(screen.getByRole('button', { name: /Back/ }));
        expect(await screen.findByText('readme.txt')).toBeTruthy();
        expect(screen.queryByRole('button', { name: /Back/ })).toBeNull();
    });

    test('sends the stored credential as Basic auth', async () => {
        renderFileList();
        await screen.findByText('readme.txt');

        expect(fetchMock.mock.calls[0][0].headers.get('Authorization')).toBe('Basic dXNlcjpwYXNz');
    });
});
