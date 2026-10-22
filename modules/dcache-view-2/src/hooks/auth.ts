import { useQuery } from '@tanstack/react-query'
import {getOIDCProviders} from "../api/authorisation.ts";

export function useOIDCProviders(){
    return useQuery({
        queryKey: ['OIDC-providers'],
        staleTime: Infinity,
        queryFn: getOIDCProviders
    })
}