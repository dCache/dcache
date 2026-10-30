import {useState} from 'react';
import {useDirectoryListing} from "../hooks/useNamespace.ts";
import {useAuthStore} from "../store/authStore.ts";
import {FileListItem} from "./FileListItem.tsx";
import {Button} from "./Button.tsx";
import {Breadcrumbs} from "./Breadcrumbs.tsx";
import type {FileAttributes} from "../types/types.ts";

function childPath(dir: string, name: string): string {
    return dir === "/" ? `/${name}` : `${dir}/${name}`;
}

function parentPath(dir: string): string {
    const parent = dir.substring(0, dir.lastIndexOf("/"));
    return parent === "" ? "/" : parent;
}

export function FileList() {
    const [dir, setDir] = useState<string>("/");
    const upauth = useAuthStore(state => state.upauth);

    const {data: entries, isPending, error} = useDirectoryListing({
        path: dir,
        upauth: upauth ? `Basic ${upauth}` : undefined,
        scope: 'partial',
        limit: 'max',
        offset: 0
    });

    if (isPending) return <p>Loading {dir} …</p>;
    if (error) return <p className="form-error">Could not list {dir}: {error.message}</p>;

    return (
        <div>
            <Breadcrumbs path={dir} onNavigate={setDir} />
            {dir !== "/" && (
                <Button label="← Back" variant="ghost" onClick={() => setDir(parentPath(dir))} />
            )}
            {entries.length === 0 ? (
                <p>This directory is empty.</p>
            ) : (
                <ul className="file-list">
                    {entries.map((entry: FileAttributes) => (
                        <li key={entry.pnfsId}>
                            <FileListItem
                                entry={entry}
                                onOpenDir={(name: string) => setDir(childPath(dir, name))}
                            />
                        </li>
                    ))}
                </ul>
            )}
        </div>
    );
}
