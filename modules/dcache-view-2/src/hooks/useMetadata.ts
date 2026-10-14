import { useQuery } from '@tanstack/react-query'
import type {FileMetadataRequest} from "../types/types.ts";
import { fileAttributes, buildHeaders } from '../api/namespace.ts'

export function useMetadata(request: FileMetadataRequest){
    return useQuery({
        queryKey: ['metadata', request.pnfsId ?? request.path],
        queryFn: () => fileAttributes(request, buildHeaders(request.upauth))
    })
}
