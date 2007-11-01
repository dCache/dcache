//______________________________________________________________________________
//
// $Id: RemoveFileCallbacks.java,v 1.1 2005-11-01 17:07:16 litvinse Exp $
// $Author: litvinse $
//
// Created 10/05 by Dmitry Litvintsev (litvinse@fnal.gov)
//______________________________________________________________________________

package org.dcache.srm;

/**
 *
 * @author  timur
 */

public interface RemoveFileCallbacks {
	public void RemoveFileSucceeded();
	public void RemoveFileFailed(String reason);
	public void Exception(Exception e);
	public void Timeout();
}
