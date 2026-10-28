import { useQuery } from '@tanstack/react-query';
import { getFileLabels } from '../api/labels.ts';
import { useAuthStore } from '../store/authStore.ts';

/** Query for the labels of a file (key ['labels', path]). */
export function useFileLabels(path: string) {
    const upauth = useAuthStore(state => state.upauth);

    return useQuery({
        queryKey: ['labels', path],
        queryFn: () => getFileLabels({ path, upauth: upauth ? `Basic ${upauth}` : undefined })
    });
}
