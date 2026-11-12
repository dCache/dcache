import { useMutation, useQueryClient } from '@tanstack/react-query';
import { fetchProviders } from '../api/authorisation.ts';
import { downloadFile } from '../api/download.ts';
import { webdavEndpoint, webdavFileUrl } from '../api/webdav.ts';
import { useAuthStore } from '../store/authStore.ts';

/** Mutation to download a file from the WebDAV door named in config.js. */
export function useDownload() {
    const queryClient = useQueryClient();
    const upauth = useAuthStore(state => state.upauth);

    return useMutation({
        mutationFn: async ({ path }: { path: string }) => {
            const config = await queryClient.fetchQuery({
                queryKey: ['view-config'],
                queryFn: fetchProviders,
                staleTime: Infinity
            });
            // anonymous login is treated as no auth
            const auth = upauth && upauth !== btoa('anonymous:nopassword') ? `Basic ${upauth}` : undefined;
            await downloadFile(webdavFileUrl(webdavEndpoint(config), path), auth);
        }
    });
}
