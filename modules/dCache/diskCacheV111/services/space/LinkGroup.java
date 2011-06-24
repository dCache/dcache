/*
 * Link.java
 *
 * Created on July 18, 2006, 1:17 PM
 *
 * To change this template, choose Tools | Template Manager
 * and open the template in the editor.
 */

package diskCacheV111.services.space;
import java.util.Date;
import diskCacheV111.util.VOInfo;

/**
 *
 * @author timur
 */
public class LinkGroup implements java.io.Serializable{
	private long id;
	private String name;
	private long freeSpace;
	private boolean onlineAllowed ;
	private boolean nearlineAllowed ;
	private boolean replicaAllowed ;
	private boolean outputAllowed;
	private boolean custodialAllowed ;
	private VOInfo[] vos;
	private long updateTime;
	private long reservedSpaceInBytes;

	public LinkGroup(){
	}

	public LinkGroup(
		long id,
		String name,
		long freeSpace,
		VOInfo[] vos,
		boolean onlineAllowed,
		boolean nearlineAllowed,
		boolean replicaAllowed,
		boolean outputAllowed,
		boolean custodialAllowed,
		long reseved) {
		this.id=id;
		this.name=name;
		this.freeSpace = freeSpace;
		this.vos = vos;
		this.onlineAllowed = onlineAllowed;
		this.nearlineAllowed = nearlineAllowed;
		this.replicaAllowed = replicaAllowed;
		this.outputAllowed = outputAllowed;
		this.custodialAllowed = custodialAllowed;
		this.reservedSpaceInBytes = reseved;
	}

	//
	// for backward compatibility
	//
	public LinkGroup(
		long id,
		String name,
		long freeSpace,
		VOInfo[] vos,
		boolean onlineAllowed,
		boolean nearlineAllowed,
		boolean replicaAllowed,
		boolean outputAllowed,
		boolean custodialAllowed) {
		this.id=id;
		this.name=name;
		this.freeSpace = freeSpace;
		this.vos = vos;
		this.onlineAllowed = onlineAllowed;
		this.nearlineAllowed = nearlineAllowed;
		this.replicaAllowed = replicaAllowed;
		this.outputAllowed = outputAllowed;
		this.custodialAllowed = custodialAllowed;
		this.reservedSpaceInBytes = 0L;
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
		StringBuffer sb = new StringBuffer();
		toStringBuffer(sb);
		return sb.toString();
	}
	public void toStringBuffer(StringBuffer sb){
		sb.append(id).append(' ');
		sb.append("Name:").append(name).append(' ');
		sb.append("FreeSpace:").append(freeSpace).append(' ');
		sb.append("ReservedSpace:").append(reservedSpaceInBytes).append(' ');
		sb.append("AvailableSpace:").append(getAvailableSpaceInBytes()).append(' ');
		sb.append("VOs:");
		for(int i = 0; i<vos.length; ++i) {
			sb.append('{').append(vos[i]).append('}');
		}
		sb.append(' ');
		sb.append("onlineAllowed:").append(onlineAllowed).append(' ');
		sb.append("nearlineAllowed:").append(nearlineAllowed).append(' ');
		sb.append("replicaAllowed:").append(replicaAllowed).append(' ');
		sb.append("custodialAllowed:").append(custodialAllowed).append(' ');
		sb.append("outputAllowed:").append(outputAllowed).append(' ');
		sb.append("UpdateTime:").append((new Date(updateTime)).toString()).append("(").append(updateTime).append(")");
	}


	public VOInfo[] getVOs() {
		return vos;
	}

	public void setVOs(VOInfo[] vos) {
		this.vos = vos;
	}

	public long getUpdateTime() {
		return updateTime;
	}

	public void setUpdateTime(long updateTime) {
		this.updateTime = updateTime;
	}

	public boolean isOnlineAllowed() {
		return onlineAllowed;
	}

	public void setOnlineAllowed(boolean onlineAllowed) {
		this.onlineAllowed = onlineAllowed;
	}

	public boolean isNearlineAllowed() {
		return nearlineAllowed;
	}

	public void setNearlineAllowed(boolean nearlineAllowed) {
		this.nearlineAllowed = nearlineAllowed;
	}

	public boolean isReplicaAllowed() {
		return replicaAllowed;
	}

	public void setReplicaAllowed(boolean replicaAllowed) {
		this.replicaAllowed = replicaAllowed;
	}

	public boolean isOutputAllowed() {
		return outputAllowed;
	}

	public void setOutputAllowed(boolean outputAllowed) {
		this.outputAllowed = outputAllowed;
	}

	public boolean isCustodialAllowed() {
		return custodialAllowed;
	}

	public void setCustodialAllowed(boolean custodialAllowed) {
		this.custodialAllowed = custodialAllowed;
	}

	public long getReservedSpaceInBytes() {
		return reservedSpaceInBytes;
	}

	public void setReservedSpaceInBytes(long reserved) {
		this.reservedSpaceInBytes = reserved;
	}
	public long getAvailableSpaceInBytes() {
		return freeSpace-reservedSpaceInBytes;
	}

}
