/*
 * Link.java
 *
 * Created on July 18, 2006, 1:17 PM
 *
 * To change this template, choose Tools | Template Manager
 * and open the template in the editor.
 */

package diskCacheV111.services.space;

/**
 *
 * @author timur
 */
public class Link implements java.io.Serializable{
    private long id;
    private String name;
    private long freeSpace;
    private String[] storageGroups;
    private String hsmType;
    /** Creates a new instance of Link */
    public Link(){
    }

    public Link(
            long id,
            String name,
            long freeSpace,
            String[] storageGroups,
            String hsmType) {
        this.id=id;
        this.name=name;
        this.freeSpace = freeSpace;
        this.storageGroups = storageGroups;
        this.hsmType = hsmType;
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public long getFreeSpace() {
        return freeSpace;
    }

    public void setFreeSpace(long freeSpace) {
        this.freeSpace = freeSpace;
    }


    public String toString(){
        StringBuilder sb = new StringBuilder();
        sb.append("Link(").append(id);
        sb.append(name).append(",").append(freeSpace).append(",");
        sb.append("StorageGroups");
        for(int i = 0; i<storageGroups.length; ++i) {
            sb.append('{').append(storageGroups[i]).append('}');
        }
        sb.append(hsmType);
        return sb.toString();
    }

    public String[] getStorageGroups() {
        return storageGroups;
    }

    public void setStorageGroups(String[] storageGroups) {
        this.storageGroups = storageGroups;
    }

    public String getHsmType() {
        return hsmType;
    }

    public void setHsmType(String hsmType) {
        this.hsmType = hsmType;
    }

}
