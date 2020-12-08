package diskCacheV111.poolManager;

import java.util.Map;

class DynamicPGroup extends PGroup {

    private static final long serialVersionUID = 3725185696861231668L;
    private final Map<String, String> tags;

    DynamicPGroup(String name, boolean resilient, Map<String, String> tags) {
        super(name, resilient);
        this.tags = tags;
    }

    public void addIfMatches(Pool pool) {

        final Pool p = pool;
        final PGroup pg = this;
        Map<String, String> poolTags = pool.getTags();
        tags.entrySet().forEach(e -> {
            String v = e.getValue();
            if (v.equals(poolTags.get(e.getKey()))) {
                p._pGroupList.put(pg.getName(), pg);
                pg._poolList.put(p.getName(), p);
            }
        });
    }

    public Map<String, String> getTags() {
        return tags;
    }

    @Override
    public String toString() {
        return super.toString() + " [dynamic]";
    }

}
