package diskCacheV111.poolManager;

import diskCacheV111.poolManager.PoolSelectionUnit.SelectionLink;
import diskCacheV111.poolManager.PoolSelectionUnit.SelectionLinkGroup;
import java.io.Serializable;
import java.util.Collection;
import java.util.concurrent.CopyOnWriteArraySet;

class LinkGroup implements SelectionLinkGroup, Serializable {

    private static final long serialVersionUID = 5425784079451748166L;
    private final String _name;
    private final Collection<SelectionLink> _links = new CopyOnWriteArraySet<>();

    /*
     * my personal view to default behavior
     */
    private boolean _isNearlineAllowed = true;
    private boolean _isOnlineAllowed;
    private boolean _isOutputAllowed = true;
    private boolean _isReplicaAllowed = true;
    private boolean _isCustodialAllowed = true;

    LinkGroup(String name) {
        _name = name;
    }

    @Override
    public String getName() {
        return _name;
    }

    @Override
    public void add(SelectionLink link) {
        _links.add(link);
    }

    @Override
    public boolean remove(SelectionLink link) {
        return _links.remove(link);
    }

    @Override
    public Collection<SelectionLink> getLinks() {
        return _links;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder(_name);
        sb.append(" : ");
        if (!_links.isEmpty()) {
            sb.append("[  ");
            for (SelectionLink link : _links) {
                sb.append(link.getName());
                sb.append(" ");
            }
            sb.append("]");
        } else {
            sb.append("[EMPTY]");
        }
        sb.append("\n");
        sb.append("    AccessLatency:\n");
        sb.append("           ").append("onlineAllowed=").append(_isOnlineAllowed).append("\n");
        sb.append("           ").append("nearlineAllowed=").append(_isNearlineAllowed).append("\n");
        sb.append("    RetentionPolicy:\n");
        sb.append("           ").append("custodialAllowed=").append(_isCustodialAllowed)
              .append("\n");
        sb.append("           ").append("outputAllowed=").append(_isOutputAllowed).append("\n");
        sb.append("           ").append("replicaAllowed=").append(_isReplicaAllowed).append("\n");
        return sb.toString();
    }

    public boolean contains(SelectionLink link) {
        return _links.contains(link);
    }

    public Collection<SelectionLink> getAllLinks() {
        return _links;
    }

    @Override
    public boolean isCustodialAllowed() {
        return _isCustodialAllowed;
    }

    @Override
    public boolean isNearlineAllowed() {
        return _isNearlineAllowed;
    }

    @Override
    public boolean isOnlineAllowed() {
        return _isOnlineAllowed;
    }

    @Override
    public boolean isOutputAllowed() {
        return _isOutputAllowed;
    }

    @Override
    public boolean isReplicaAllowed() {
        return _isReplicaAllowed;
    }

    @Override
    public void setCustodialAllowed(boolean isAllowed) {
        _isCustodialAllowed = isAllowed;
    }

    @Override
    public void setNearlineAllowed(boolean isAllowed) {
        _isNearlineAllowed = isAllowed;
    }

    @Override
    public void setOnlineAllowed(boolean isAllowed) {
        _isOnlineAllowed = isAllowed;
    }

    @Override
    public void setOutputAllowed(boolean isAllowed) {
        _isOutputAllowed = isAllowed;
    }

    @Override
    public void setReplicaAllowed(boolean isAllowed) {
        _isReplicaAllowed = isAllowed;
    }

}
