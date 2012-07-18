/*
 * PoolLinkInfo.java
 *
 * Created on August 3, 2006, 4:56 PM
 *
 * To change this template, choose Tools | Template Manager
 * and open the template in the editor.
 */

package diskCacheV111.vehicles;

/**
 *
 * @author timur
 */
public class PoolLinkInfo implements java.io.Serializable {
    private String name;
    private long availableSpaceInBytes;
    private String[] storageGroups;
    private String hsmType="None";

  // need to add once finalized
   // private static final long serialVersionUID = ;
    /** Creates a new instance of PoolLinkInfo */
    public PoolLinkInfo() {
    }

    public PoolLinkInfo(String name, long availableSpaceInBytes, String[] storageGroups){
        this.name = name;
        this.availableSpaceInBytes = availableSpaceInBytes;
        this.storageGroups = storageGroups;
    }

    public PoolLinkInfo(String name,
            long availableSpaceInBytes,
            String[] storageGroups,
            String hsmType){
        this.name = name;
        this.availableSpaceInBytes = availableSpaceInBytes;
        this.storageGroups = storageGroups;
        this.hsmType = hsmType;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public long getAvailableSpaceInBytes() {
        return availableSpaceInBytes;
    }

    public void setAvailableSpaceInBytes(long availableSpaceInBytes) {
        this.availableSpaceInBytes = availableSpaceInBytes;
    }

    public String[] getStorageGroups() {
        return storageGroups;
    }

    public void setStorageGroups(String[] storageGroups) {
        this.storageGroups = storageGroups;
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Link(").append(name).append(",").append(availableSpaceInBytes).append(",");
        sb.append("StorageGroups");
        for(int i = 0; i<storageGroups.length; ++i) {
            sb.append('{').append(storageGroups[i]).append('}');
        }
        sb.append(hsmType);
        return sb.toString();
    }

    public String getHsmType() {
        return hsmType;
    }

    public void setHsmType(String hsmType) {
        this.hsmType = hsmType;
    }

}
