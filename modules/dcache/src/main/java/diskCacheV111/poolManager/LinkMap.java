package diskCacheV111.poolManager;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

class LinkMap {

    static class LinkMapEntry {

        final Link _link;
        int _counter;

        LinkMapEntry(Link link) {
            _link = link;
            _counter = link._uGroupList.size() - 1;
        }
    }

    private final Map<String, LinkMapEntry> _linkHash = new HashMap<>();

    Iterator<Link> iterator() {
        return _linkHash.values().stream()
              .filter(e -> e._counter <= 0)
              .map(e -> e._link)
              .iterator();
    }

    void addLink(Link link) {
        LinkMapEntry found = _linkHash.get(link.getName());
        if (found == null) {
            _linkHash.put(link.getName(), new LinkMapEntry(link));
        } else {
            found._counter--;
        }
    }

    Collection<LinkMapEntry> entries() {
        return _linkHash.values();
    }
}
