package diskCacheV111.util;

import java.util.*;

public class FsPath {

    private final List<String> _list;

    public List<String> getPathItemsList() {
        return new ArrayList<String>(_list);
    }

    public FsPath(FsPath path) {
        _list = new ArrayList<String>(path._list);
    }

    public FsPath(String path) {
        _list = new ArrayList<String>();
        add(path);
    }

    public String toString() {
        return toString(_list);
    }

    public static String toString(List<String> pathItemsList) {
        if (pathItemsList.isEmpty())
            return "/";
        StringBuilder sb = new StringBuilder();

        for (String pathItem : pathItemsList) {
            sb.append("/").append(pathItem);
        }
        return sb.toString();
    }

    public void add(String path) {
        if ((path == null) || (path.length() == 0))
            return;
        if (path.startsWith("/")) {
            _list.clear();
        }
        StringTokenizer st = new StringTokenizer(path, "/");
        while (st.hasMoreTokens()) {
            addSingleItem(st.nextToken());
        }
        return;

    }

    private void addSingleItem(String item) {
        if (item.equals("."))
            return;
        if (item.equals("..")) {
            if (_list.size() > 0)
                _list.remove(_list.size() - 1);
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
