//______________________________________________________________________________
//
// $Id: RemoveFileCallbacks.java,v 1.2 2007-08-16 22:06:19 timur Exp $
// $Author: timur $
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
        public void FileNotFound(String error);
	public void Exception(Exception e);
	public void Timeout();
}
