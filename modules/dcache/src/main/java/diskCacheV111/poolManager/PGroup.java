package diskCacheV111.poolManager;

import com.google.common.graph.GraphBuilder;
import com.google.common.graph.Graphs;
import com.google.common.graph.MutableGraph;
import diskCacheV111.poolManager.PoolSelectionUnit.SelectionPoolGroup;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

class PGroup extends PoolCore implements SelectionPoolGroup {

    private static final long serialVersionUID = 3883973457610397314L;
    final Map<String, Pool> _poolList = new ConcurrentHashMap<>();
    final List<PGroup> _pgroupList = new CopyOnWriteArrayList<>();

    private final boolean resilient;

    PGroup(String name, boolean resilient) {
        super(name);
        this.resilient = resilient;
    }

    @Override
    public boolean isResilient() {
        return resilient;
    }

    @Override
    public boolean isPrimary() {
        return resilient;
    }

    @Override
    public String toString() {
        return getName() + "(links=" + _linkList.size()
              + "; pools=" + _poolList.size() + "; resilient=" + resilient + "; nested groups=" + _pgroupList + ")";
    }

    @Override
    public List<Pool> getPools() {
        List<Pool> allPools = new ArrayList<>(_poolList.values());
        _pgroupList.forEach(g -> allPools.addAll(g.getPools()));
        return allPools;
    }


    // check whatever there is a pool group that exist in there reference list
    // IOW, A ->  B -> C ; C -> A not allowed.
    private void checkLoop(PGroup top, PGroup subGroup) {
        var g = GraphBuilder
              .directed()
              .allowsSelfLoops(true) // we will check later
              .build();

        fillReferenceTree(top, subGroup, g);

        if (Graphs.hasCycle(g)) {
            throw new IllegalArgumentException("Cyclic pool group reference not allowed: " + g.edges());
        }
    }

    private void fillReferenceTree(PGroup top, PGroup subGroup, MutableGraph g) {

        if (!g.putEdge(top.getName(), subGroup.getName())) {
            // the edge is not added, probably a duplicate
            throw new IllegalArgumentException("Subgroup " + subGroup.getName() + " is already defined in " + top.getName());
        }

        for (PGroup ref : subGroup._pgroupList) {
            if (!g.putEdge(subGroup.getName(), ref.getName())) {
                // the edge is not added, probably a duplicate
                throw new IllegalArgumentException("Subgroup " + ref.getName() + " is already defined in " + subGroup.getName());
            }
            fillReferenceTree(subGroup, ref, g);
        }
    }

    public void addSubgroup(PGroup subGroup) {
        checkLoop(this, subGroup);
        _pgroupList.add(subGroup);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PGroup group = (PGroup) o;
        return getName().equals(group.getName());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getName());
    }
}
