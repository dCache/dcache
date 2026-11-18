import { useMutation, useQueryClient } from '@tanstack/react-query';
import { fetchProviders } from '../api/authorisation.ts';
import { uploadFile } from '../api/upload.ts';
import { webdavEndpoint, webdavFileUrl } from '../api/webdav.ts';
import { useAuthStore } from '../store/authStore.ts';

interface UploadRequest {
    folder: string;
    file: File;
    onProgress?: (percent: number) => void;
}

/** Mutation to upload a file into a folder through the WebDAV door named in config.js. */
export function useUpload() {
    const queryClient = useQueryClient();
    const upauth = useAuthStore(state => state.upauth);

    return useMutation({
        mutationFn: async ({ folder, file, onProgress }: UploadRequest) => {
            const config = await queryClient.fetchQuery({
                queryKey: ['view-config'],
                queryFn: fetchProviders,
                staleTime: Infinity
            });
            // anonymous login is treated as no auth
            const auth = upauth && upauth !== btoa('anonymous:nopassword') ? `Basic ${upauth}` : undefined;
            const url = webdavFileUrl(webdavEndpoint(config), `${folder}/${file.name}`);
            await uploadFile(url, file, auth, onProgress);
        },
        // reload the folder, so the new file shows up in the list
        onSuccess: (_data, variables) =>
            queryClient.invalidateQueries({ queryKey: ['directory-listing', variables.folder] })
    });
}
