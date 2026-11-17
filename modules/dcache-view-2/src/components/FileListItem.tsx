import type {FileAttributes} from "../types/types.ts";

interface FileListItemProps {
    entry: FileAttributes;
    onOpenDir: (name: string) => void;
}

export function FileListItem({ entry, onOpenDir }: FileListItemProps) {
    const name = entry.fileName!;

    if (entry.fileType === 'DIR') {
        return (
            <button
                type="button"
                className="file-list__item file-list__item--dir"
                onClick={() => onOpenDir(name)}
            >
                <span>📁</span>
                <span>{name}</span>
            </button>
        );
    }

    return (
        <div className="file-list__item">
            <span>📄</span>
            <span>{name}</span>
        </div>
    );
}
