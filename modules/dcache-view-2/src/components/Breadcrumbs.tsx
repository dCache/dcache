interface BreadcrumbsProps {
    path: string;
    onNavigate: (path: string) => void;
}

export function Breadcrumbs({ path, onNavigate }: BreadcrumbsProps) {
    const segments = path.split("/").filter((segment: string) => segment !== "");
    const crumbs = [
        { name: "/", path: "/" },
        ...segments.map((segment: string, i: number) => ({
            name: segment,
            path: "/" + segments.slice(0, i + 1).join("/")
        }))
    ];

    return (
        <nav aria-label="Breadcrumb">
            <ol className="breadcrumbs">
                {crumbs.map((crumb, i: number) => {
                    const isCurrent = i === crumbs.length - 1;
                    return (
                        <li key={crumb.path} className="breadcrumbs__item">
                            {isCurrent ? (
                                <span className="breadcrumbs__current" aria-current="page">{crumb.name}</span>
                            ) : (
                                <button
                                    type="button"
                                    className="breadcrumbs__link"
                                    onClick={() => onNavigate(crumb.path)}
                                >
                                    {crumb.name}
                                </button>
                            )}
                        </li>
                    );
                })}
            </ol>
        </nav>
    );
}
