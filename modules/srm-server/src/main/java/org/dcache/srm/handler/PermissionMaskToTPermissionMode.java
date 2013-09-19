
package org.dcache.srm.handler;
import org.dcache.srm.v2_2.TPermissionMode;

public class PermissionMaskToTPermissionMode{ 
	public static TPermissionMode maskToTPermissionMode(int permMask) {
		switch(permMask) {
		case 0: return TPermissionMode.NONE;
		case 1: return TPermissionMode.X;
		case 2: return TPermissionMode.W;
		case 3: return TPermissionMode.WX;
		case 4: return TPermissionMode.R;
		case 5: return TPermissionMode.RX;
		case 6: return TPermissionMode.RW;
		case 7: return TPermissionMode.RWX;
		default:
			throw new IllegalArgumentException("illegal perm mask: "+permMask);
		}
	}
	public static int permissionModetoMask(TPermissionMode mode) {
		if (mode.getValue().equalsIgnoreCase(TPermissionMode.NONE.getValue())) { 
			return 0;
		}
		else if ( mode.getValue().equalsIgnoreCase(TPermissionMode.X.getValue())) { 
			return 1;
		}
		else if ( mode.getValue().equalsIgnoreCase(TPermissionMode.W.getValue())) { 
			return 2;
		}
		else if ( mode.getValue().equalsIgnoreCase(TPermissionMode.WX.getValue())) { 
			return 3;
		}
		else if ( mode.getValue().equalsIgnoreCase(TPermissionMode.R.getValue())) { 
			return 4;
		}
		else if ( mode.getValue().equalsIgnoreCase(TPermissionMode.RX.getValue())) { 
			return 5;
		}
		else if ( mode.getValue().equalsIgnoreCase(TPermissionMode.RW.getValue())) { 
			return 6;
		}
		else if ( mode.getValue().equalsIgnoreCase(TPermissionMode.RWX.getValue())) { 
			return 7;
		}
		else { 
			return 0;
		}
	}
}
