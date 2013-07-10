package diskCacheV111.cells;

import java.io.PrintWriter;
import java.util.List;
import java.util.Map;

/**
	For each request type (read/write), there is an initial
	and final decision method called. These methods carry the
	original request message and list of pools togheder with
	their PoolQualities. The actual decision work is done
	within these methods. The SelectionClass is assumed to remove
	as many pools as possible from the list and only let the
	best candidates survive.

	In addition implementaion requaries constructur to be defined
	as well, to allow automatic classloading.
*/

public interface PoolSelectable {


	// The decision methods

	/**
	    Write Pool preselection on available static information
	*/

	public void initialWritePoolSelection( PoolSelectionRequest request, List poolQualitylist);

	/**
	    Write Pool final selection on dynamic informatio
	    e.q. space cost, perfomance and preferences.

	*/

	public void finalWritePoolSelection( PoolSelectionRequest request, List poolQualitylist);

	/**
	    Read Pool preselection on available static information
	*/

	public void initialReadPoolSelection( PoolSelectionRequest request, List poolQualitylist);

	/**
	    Read Pool final selection on dynamic informatio
	    e.q. space cost, perfomance and preferences.

	*/

	public void finalReadPoolSelection( PoolSelectionRequest request, List poolQualitylist);


	// Utility methods
	public void setParameter(String key, Object value);
	public Object getParameter(String key);
	public void printInfo( PrintWriter printWriter);
	public Map getEnviroment();
}
