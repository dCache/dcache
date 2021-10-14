package diskCacheV111.services.space;


import diskCacheV111.util.VOInfo;
import java.util.List;

public class LinkGroupAuthorizationRecord {

    private final String linkGroupName;
    final List<VOInfo> voinfos;


    public LinkGroupAuthorizationRecord(String linkGroupName,
          List<VOInfo> voinfos) {
        this.linkGroupName = linkGroupName;
        this.voinfos = voinfos;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(getLinkGroupName());
        for (VOInfo voinfo : voinfos) {
            sb.append(", ");
            sb.append(voinfo);
        }
        return sb.toString();
    }

    public String getLinkGroupName() {
        return linkGroupName;
    }

    public VOInfo[] getVOInfoArray() {
        return voinfos.toArray(VOInfo[]::new);
    }
}




