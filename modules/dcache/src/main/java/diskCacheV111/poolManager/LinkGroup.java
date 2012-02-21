package diskCacheV111.poolManager;

import diskCacheV111.poolManager.PoolSelectionUnit.SelectionLink;
import diskCacheV111.poolManager.PoolSelectionUnit.SelectionLinkGroup;
import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

class LinkGroup implements SelectionLinkGroup, Serializable {
    static final long serialVersionUID = 5425784079451748166L;
    private final String _name;
    private final Collection<SelectionLink> _links = new HashSet<SelectionLink>();
    // no duplicates is allowed
    private final Map<String, Set<String>> _attributes = new HashMap<String, Set<String>>();
    /*
     * my personal view to default behavior
     */
    private boolean _isNearlineAllowed = true;
    private boolean _isOnlineAllowed = false;
    private boolean _isOutputAllowed = true;
    private boolean _isReplicaAllowed = true;
    private boolean _isCustodialAllowed = true;

    LinkGroup(String name) {
        _name = name;
    }

    public String getName() {
        return _name;
    }

    public void add(SelectionLink link) {
        _links.add(link);
    }

    public boolean remove(SelectionLink link) {
        return _links.remove(link);
    }

    public Collection<SelectionLink> links() {
        return _links;
    }

    public void attribute(String attribute, String value, boolean replace) {
        Set<String> valuesSet = null;
        if (!_attributes.containsKey(attribute)) {
            valuesSet = new HashSet<String>();
            _attributes.put(attribute, valuesSet);
        } else {
            valuesSet = _attributes.get(attribute);
            if (replace) {
                valuesSet.clear();
            }
        }
        valuesSet.add(value);
    }

    public Set<String> attribute(String attribute) {
        return _attributes.get(attribute);
    }

    /**
     *
     * remove a value associated with a attribute if attribute is empty,
     * remove attribute as well.
     *
     * @param attribute
     * @param value
     */
    public void removeAttribute(String attribute, String value) {
        if (_attributes.containsKey(attribute)) {
            Set<String> valuesSet = _attributes.get(attribute);
            valuesSet.remove(value);
            if (valuesSet.isEmpty()) {
                _attributes.remove(attribute);
            }
        }
    }

    public Map<String, Set<String>> attributes() {
        return new HashMap<String, Set<String>>(_attributes);
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
        sb.append("    Attributes:\n");
        for (Map.Entry<String, Set<String>> aAttribute : _attributes.entrySet()) {
            sb.append("           ").append(aAttribute.getKey()).append(" = ");
            for (String aAttributeValue : aAttribute.getValue()) {
                sb.append(aAttributeValue).append(" ");
            }
            sb.append("\n");
        }
        sb.append("    AccessLatency:\n");
        sb.append("           ").append("onlineAllowed=").append(_isOnlineAllowed).append("\n");
        sb.append("           ").append("nearlineAllowed=").append(_isNearlineAllowed).append("\n");
        sb.append("    RetentionPolicy:\n");
        sb.append("           ").append("custodialAllowed=").append(_isCustodialAllowed).append("\n");
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

    public boolean isCustodialAllowed() {
        return _isCustodialAllowed;
    }

    public boolean isNearlineAllowed() {
        return _isNearlineAllowed;
    }

    public boolean isOnlineAllowed() {
        return _isOnlineAllowed;
    }

    public boolean isOutputAllowed() {
        return _isOutputAllowed;
    }

    public boolean isReplicaAllowed() {
        return _isReplicaAllowed;
    }

    public void setCustodialAllowed(boolean isAllowed) {
        _isCustodialAllowed = isAllowed;
    }

    public void setNearlineAllowed(boolean isAllowed) {
        _isNearlineAllowed = isAllowed;
    }

    public void setOnlineAllowed(boolean isAllowed) {
        _isOnlineAllowed = isAllowed;
    }

    public void setOutputAllowed(boolean isAllowed) {
        _isOutputAllowed = isAllowed;
    }

    public void setReplicaAllowed(boolean isAllowed) {
        _isReplicaAllowed = isAllowed;
    }

}
