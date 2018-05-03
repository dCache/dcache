package diskCacheV111.vehicles;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import diskCacheV111.util.PnfsId;

public class PnfsGetParentMessage extends PnfsMessage
{
    private static final long serialVersionUID = 7665073616113975562L;
    private PnfsId _parent;
    private List<PnfsId> parents;
    private List<String> names;

    public PnfsGetParentMessage(PnfsId pnfsId)
    {
        super(pnfsId);
        setReplyRequired(true);
    }

    public void addLocation(PnfsId parent, String name)
    {
        if (parents == null) {
            parents = new ArrayList<>();
        }
        if (names == null) {
            names = new ArrayList<>();
        }
        if (_parent == null) {
            _parent = parent;
        }
        parents.add(parent);
        names.add(name);
    }

    public PnfsId getParent()
    {
        return _parent;
    }

    public List<PnfsId> getParents()
    {
        return parents == null ? Collections.emptyList() : parents;
    }

    public List<String> getNames()
    {
        return names == null ? Collections.emptyList() : names;
    }

    @Override
    public boolean invalidates(Message message)
    {
        return false;
    }
}
