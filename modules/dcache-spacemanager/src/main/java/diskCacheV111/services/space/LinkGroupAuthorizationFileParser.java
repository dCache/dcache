package diskCacheV111.services.space;

import diskCacheV111.util.VOInfo;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import org.dcache.util.files.LineBasedParser;


public class LinkGroupAuthorizationFileParser
        implements LineBasedParser<Map<String,LinkGroupAuthorizationRecord>> {

    private static final int OUTSIDE_STATE = 0;
    private static final int LINKGROUP_STATE = 1;
    private static final String COMMENT_KEY = "#";
    private static final String LINKGROUP_KEY = "LinkGroup";

    private final Map<String, LinkGroupAuthorizationRecord> records =
          new HashMap<>();

    private int state = OUTSIDE_STATE;
    private String linkGroupName = null;
    private List<VOInfo> voinfos = null;

    @Override
    public void accept(String line) throws UnrecoverableParsingException {
        line = line.trim();
        if (line.startsWith(COMMENT_KEY)) {
            return;
        }
        if (state == OUTSIDE_STATE) {
            if (line.length() == 0) {
                return;
            }
            StringTokenizer st = new StringTokenizer(line);
            if (!LINKGROUP_KEY.equals(st.nextToken())) {
                throw new UnrecoverableParsingException("First token must be " +
                      LINKGROUP_KEY);
            }
            state = LINKGROUP_STATE;
            linkGroupName = st.nextToken();
            voinfos = new ArrayList<>();
        } else if (state == LINKGROUP_STATE) {
            if (line.length() == 0) {
                LinkGroupAuthorizationRecord record =
                      new LinkGroupAuthorizationRecord(
                            linkGroupName, voinfos);
                records.put(linkGroupName, record);
                state = OUTSIDE_STATE;
                voinfos = null;
                linkGroupName = null;
                return;
            }
            VOInfo voinfo = new VOInfo(line);
            voinfos.add(voinfo);
        }
    }

    @Override
    public Map<String, LinkGroupAuthorizationRecord> build() {
        if (state == LINKGROUP_STATE) {
            LinkGroupAuthorizationRecord record =
                  new LinkGroupAuthorizationRecord(
                        linkGroupName, voinfos);
            records.put(linkGroupName, record);
        }

        return records;
    }
}
