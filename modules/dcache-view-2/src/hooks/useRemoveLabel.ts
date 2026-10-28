import { useMutation, useQueryClient } from '@tanstack/react-query';
import { removeLabel } from '../api/labels.ts';
import { useAuthStore } from '../store/authStore.ts';
import type { LabelRequest } from '../types/types.ts';

/** Mutation to remove a label; reloads ['metadata', path] and ['labels', path] on success. */
export function useRemoveLabel() {
    const queryClient = useQueryClient();
    const upauth = useAuthStore(state => state.upauth);

    return useMutation({
        mutationFn: ({ path, label }: Omit<LabelRequest, 'upauth'>) =>
            removeLabel({ path, label, upauth: upauth ? `Basic ${upauth}` : undefined }),
        onSuccess: (_data, variables) => Promise.all([
            queryClient.invalidateQueries({ queryKey: ['metadata', variables.path] }),
            queryClient.invalidateQueries({ queryKey: ['labels', variables.path] })
        ])
    });
}
