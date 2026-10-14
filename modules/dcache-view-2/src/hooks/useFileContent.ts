import type {FileContentRequest} from "../types/types.ts";
import {useQuery} from "@tanstack/react-query";
import {fileContent} from "../api/file-content.ts";

export function useFileContent(request: FileContentRequest) {
    return useQuery({
        queryKey: ['metadata', request.url],
        queryFn: () => fileContent(request)
    })
}