package org.dcache.services.info.gathers;

import org.apache.log4j.Logger;
import org.dcache.services.info.base.StateExhibitor;
import org.dcache.services.info.base.StatePath;

import dmg.cells.nucleus.CellMessageAnswerable;
import dmg.cells.nucleus.CellPath;

public class CellInfoDga extends SkelListBasedActivity {

	private static final Logger _log = Logger.getLogger( CellInfoDga.class);

	private final MessageSender _sender;

	/**
	 *  Use our own list timings.  Enforce a minimum delay of two minutes between successive
	 *  getcellinfos requests to the *same* domain, and a delay of at least two seconds between
	 *  successive requests of information from any domain.
	 */
	private static int MIN_LIST_REFRESH_PERIOD = 120000;
	private static int SUCC_MSG_DELAY = 2000;

	private final CellMessageAnswerable _handler;

	public CellInfoDga( StateExhibitor exhibitor, MessageSender sender, CellMessageAnswerable handler) {

		super( exhibitor, new StatePath( "domains"), MIN_LIST_REFRESH_PERIOD, SUCC_MSG_DELAY);

		_handler = handler;
		_sender = sender;
	}

	/**
	 * Method called periodically when we should send out a message.
	 */
	@Override
	public void trigger() {
		super.trigger();

		String domainName = getNextItem();

		// This can happen, indicating that there's nothing to do.
		if( domainName == null)
			return;


		CellPath systemCellPath = new CellPath( "System", domainName);

		if( _log.isInfoEnabled())
			_log.info( "sending message getcellinfos to System cell on domain " + domainName);

		_sender.sendMessage( getMetricLifetime(), _handler, systemCellPath, "getcellinfos");
	}

	/**
	 * We only expect to have a single instance of this class.
	 */
	@Override
    public String toString() {
		return this.getClass().getSimpleName();
	}

}
