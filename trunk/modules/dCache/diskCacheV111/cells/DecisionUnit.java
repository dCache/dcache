package diskCacheV111.cells;

import  diskCacheV111.vehicles.*;
import  diskCacheV111.util.*;
import  diskCacheV111.movers.* ;
import  dmg.cells.nucleus.*;
import  dmg.util.*;

import  java.util.*;
import  java.io.*;
import  java.net.*;
import  java.lang.reflect.* ;

/**
   The DicisionUnit is the instance which actually decides
   which pool is the most appropriate for a given request.

*/

public class DecisionUnit implements PoolSelectable {

	Map _enviroment;
	
	public DecisionUnit() {};
	
	// The decision methods
	
	// Preselection methods
	public void initialWritePoolSelection( PoolSelectionRequest request, List poolQualitylist) {
	};

	public void initialReadPoolSelection( PoolSelectionRequest request , List poolQualitylist) {
	};

	// Final decision methods
	public void finalWritePoolSelection( PoolSelectionRequest request, List poolQualitylist) {
	};
	
	public void finalReadPoolSelection( PoolSelectionRequest request, List poolQualitylist) {
	} ;

	// Utility methods
	public void setParameter(String key, Object value) {};
	
	public Object getParameter(String key) {
		return new Object();
	};
	
	public void printInfo( PrintWriter printWriter) {};
	public Map getEnviroment() {
		return _enviroment;
	};
}
