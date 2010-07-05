package diskCacheV111.services.space;



import java.util.List;
import diskCacheV111.util.VOInfo;

public class LinkGroupAuthorizationRecord {
	private String linkGroupName;
	List<VOInfo> voinfos;


	public LinkGroupAuthorizationRecord(String linkGroupName,
				 List<VOInfo>  voinfos) {
		this.linkGroupName=linkGroupName;
		this.voinfos = voinfos;
	}

    @Override
    public String toString() {
		StringBuffer sb = new StringBuffer();
		sb.append(getLinkGroupName());
                for(VOInfo voinfo:voinfos) {
                    sb.append(", ");
                    sb.append(voinfo);
                }
		return sb.toString();
	}

    public String getLinkGroupName() {
        return linkGroupName;
    }

    private static final VOInfo[] zerro = new VOInfo[0];
    public VOInfo[]  getVOInfoArray() {
        return voinfos.toArray(zerro);
    }
}




