import {useQuery, type UseQueryResult} from '@tanstack/react-query'
import type {FileAttributes, FileMetadataRequest} from "../types/types.ts";
import {fileAttributes, buildHeaders, ls} from '../api/namespace.ts'

export function useMetadata(request: FileMetadataRequest): UseQueryResult<FileAttributes, Error> {
    return useQuery({
        queryKey: ['metadata', request.pnfsId ?? request.path],
        queryFn: () => fileAttributes(request, buildHeaders(request.upauth))
    })
}

export function useDirectoryListing(request: FileMetadataRequest): UseQueryResult<FileAttributes[], Error> {
    return useQuery({
        queryKey: ['directory-listing', request.path],
        queryFn: () => fileAttributes(request, buildHeaders(request.upauth)),
        select: ls // result of fileAttributes() into ls and reshape the return type to FileAttributes[]
    });
}