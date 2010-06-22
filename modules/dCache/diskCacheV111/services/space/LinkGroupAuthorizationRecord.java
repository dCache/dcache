package diskCacheV111.services.space;


import java.awt.geom.Arc2D;
import java.util.*;
import java.io.*;
import java.lang.*;
import org.dcache.auth.FQAN;

public class LinkGroupAuthorizationRecord {
	private String linkGroupName;
	List<FQAN> fqans;

	
	public LinkGroupAuthorizationRecord(String linkGroupName, 
				 List<FQAN>  fqans) { 
		this.linkGroupName=linkGroupName;
		this.fqans = fqans;
	}

	public String toString() { 
		StringBuffer sb = new StringBuffer();
		sb.append(getLinkGroupName());
                for(FQAN fqan:fqans) {
                    sb.append(", ");
                    sb.append(fqan);
                }
		return sb.toString();
						  
	}

    public String getLinkGroupName() {
        return linkGroupName;
    }

    public List<FQAN>  getFqans() {
        return fqans;
    }
    
    private static final FQAN[] zerro = new FQAN[0];
    public FQAN[]  getFqanArray() {
        return fqans.toArray(zerro);
    }
	
}




