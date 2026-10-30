import { afterEach, describe, expect, test, vi } from 'vitest';
import { cleanup, fireEvent, render, screen } from '@testing-library/react';
import { Breadcrumbs } from '../Breadcrumbs.tsx';

afterEach(cleanup);

describe('Breadcrumbs', () => {
    test('navigates to the full path of the clicked ancestor', () => {
        const onNavigate = vi.fn();
        render(<Breadcrumbs path="/a/b/c" onNavigate={onNavigate} />);

        fireEvent.click(screen.getByRole('button', { name: 'b' }));
        fireEvent.click(screen.getByRole('button', { name: '/' }));

        expect(onNavigate.mock.calls).toEqual([['/a/b'], ['/']]);
    });

    test('current directory is not clickable', () => {
        render(<Breadcrumbs path="/a/b" onNavigate={vi.fn()} />);

        expect(screen.queryByRole('button', { name: 'b' })).toBeNull();
        expect(screen.getByText('b').getAttribute('aria-current')).toBe('page');
    });
});
