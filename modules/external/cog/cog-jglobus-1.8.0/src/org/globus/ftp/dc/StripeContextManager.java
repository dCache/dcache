/*
 * Copyright 1999-2006 University of Chicago
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.globus.ftp.dc;
import org.globus.ftp.extended.GridFTPServerFacade;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class StripeContextManager {

	static Log logger =
		LogFactory.getLog(StripeContextManager.class.getName());

	protected int stripes;
	protected StripeTransferContext contextList[];
	protected int stripeQuitTokens = 0;
	protected Object contextQuitToken = new Object();

	public StripeContextManager(int stripes, 
				    SocketPool pool, 
				    GridFTPServerFacade facade) {
		this.stripes = stripes;
		contextList = new StripeTransferContext[stripes];
		for (int i = 0; i < stripes; i++) {
			contextList[i] = new StripeTransferContext(this);
			contextList[i].setSocketPool(pool);
			contextList[i].setTransferThreadManager(
			    facade.createTransferThreadManager());
		}
	}

	/**
	   return number of stripes
	 **/
	public int getStripes() {
		return stripes;
	}

	public EBlockParallelTransferContext getStripeContext(int stripe) {
		return contextList[stripe];
	}

	public Object getQuitToken() {
		int i = 0;
		while (i < stripes) {
			logger.debug("examining stripe " + i);
			if (contextList[i].getStripeQuitToken() != null) {
				// obtained quit token from one stripe.
				stripeQuitTokens ++;
				logger.debug("obtained stripe quit token. Total = " + stripeQuitTokens + "; total needed = " + stripes);
			}
			i ++;
		}
		if (stripeQuitTokens == stripes) {
			// obtained quit tokens from all stripes.
			// ready to release the quit token. But make sure not to do it twice.
			// This section only returns non-nul the first time it is entered.
			if (contextQuitToken == null) {
			    logger.debug("not releasing the quit token.");
			} else {
			    logger.debug("releasing the quit token.");
			}
			Object myToken = contextQuitToken;
			contextQuitToken = null;
			return myToken;
		} else {
			// not all stripes ready to quit
			logger.debug("not releasing the quit token. ");
			return null;
		}
	}

	class StripeTransferContext extends EBlockParallelTransferContext {

		StripeContextManager mgr;

		public StripeTransferContext(StripeContextManager mgr) {
			this.mgr = mgr;
		}

		/**
		   @return non-null if this stripe received or sent all the EODs
		**/
		public Object getStripeQuitToken() {
			Object token = super.getQuitToken();
			StripeContextManager.logger.debug(
				(token != null) ? "stripe released the quit token" : "stripe did not release the quit token");
			return token;
		}

		/**
		   @return non-null if all EODs in all stripes have been transferred.
		**/
		public Object getQuitToken() {
			return mgr.getQuitToken();
		}
	}
}
