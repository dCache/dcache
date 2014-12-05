package diskCacheV111.util;

import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

public class FsPath {

    private final List<String> _list;

    public List<String> getPathItemsList() {
        return new ArrayList<>(_list);
    }

    public FsPath(String path) {
        this();
        add(path);
    }

    public FsPath()
    {
        _list = new ArrayList<>();
    }

    public FsPath(FsPath path) {
        this(path._list);
    }

    public FsPath(FsPath path, String child)
    {
        this(path._list);
        addSingleItem(child);
    }

    public FsPath(FsPath path, String... children)
    {
        this(path._list);
        for (String child : children) {
            addSingleItem(child);
        }
    }

    public FsPath(List<String> list)
    {
        _list = new ArrayList<>(list);
    }

    public FsPath(FsPath... paths) {
        int length = 0;
        for (FsPath path: paths) {
            length += path._list.size();
        }
        _list = new ArrayList<>(length);
        for (FsPath path: paths) {
            _list.addAll(path._list);
        }
    }

    public String toString() {
        return toString(_list);
    }

    public static String toString(List<String> pathItemsList) {
        if (pathItemsList.isEmpty()) {
            return "/";
        }
        StringBuilder sb = new StringBuilder();

        for (String pathItem : pathItemsList) {
            sb.append("/").append(pathItem);
        }
        return sb.toString();
    }

    public void add(String path) {
        if ((path == null) || (path.length() == 0)) {
            return;
        }
        if (path.startsWith("/")) {
            _list.clear();
        }
        StringTokenizer st = new StringTokenizer(path, "/");
        while (st.hasMoreTokens()) {
            addSingleItem(st.nextToken());
        }
    }

    private void addSingleItem(String item) {
        if (item.equals(".")) {
            return;
        }
        if (item.equals("..")) {
            if (_list.size() > 0) {
                _list.remove(_list.size() - 1);
            }
            return;
        }
        _list.add(item);
    }

    public boolean isEmpty() {
        return _list.isEmpty();
    }

    public void pop() {
        if (!_list.isEmpty()) {
            _list.remove(_list.size() - 1);
        }
    }

    public int hashCode() {
        return _list.hashCode();
    }

    public boolean equals(Object o) {
        return (o instanceof FsPath) && _list.equals(((FsPath) o)._list);
    }

    /**
     * Returns the simple name of the path, that is, the last path
     * element.
     */
    public String getName()
    {
        if (_list.isEmpty()) {
            return "";
        } else {
            return _list.get(_list.size() - 1);
        }
    }

    public FsPath getParent()
    {
        if (isEmpty()) {
            throw new IllegalStateException("Root does not have a parent");
        }
        return new FsPath(_list.subList(0, _list.size() - 1));
    }

    public boolean startsWith(FsPath prefix)
    {
        if (prefix._list.size() > _list.size()) {
            return false;
        }

        for (int i = 0; i < prefix._list.size(); i++) {
            if (!prefix._list.get(i).equals(_list.get(i))) {
                return false;
            }
        }
        return true;
    }

    /**
     * Relativizes the given FsPath against this FsPath.
     *
     * The relativization of the given FsPath against this FsPath is
     * computed as follows:
     *
     * 1. If the path of this FsPath is not a prefix of the path of
     *    the given FsPath, then IllegalArgumentException is thrown.
     *
     * 2. Otherwise a new relative FsPath is constructed with a path
     *    computed by removing this FsPath's path from the beginning
     *    of the given FsPath's path.
     *
     * @param path The FsPath to be relativized against this FsPath
     * @return The resulting FsPath
     * @throw NullPointerException If path is null
     * @throw IllegalArgumentException If this FsPath is not a prefix
     * of the given FsPath
     */
    public FsPath relativize(FsPath path)
    {
        if (!path.startsWith(this)) {
            throw new IllegalArgumentException(toString() + " is not a prefix of " + path);
        }
        return new FsPath(path._list.subList(_list.size(), path._list.size()));
    }

    /**
     * Returns the parent path of a path.
     */
    public static String getParent(String path)
    {
        FsPath p = new FsPath(path);
        return p.getParent().toString();
    }

    public static void main(String[] args) {
        FsPath path = new FsPath("/pnfs/desy.de");
        System.out.println(path.toString());
        path.add("zeus/users/patrick");
        System.out.println(path.toString());
        path.add("../trude");
        System.out.println(path.toString());
        path.add("/");
        System.out.println(path.toString());
        path.add("pnfs/cern.ch");
        System.out.println(path.toString());
        path.add("./../././");
        System.out.println(path.toString());
    }
}
