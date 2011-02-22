//______________________________________________________________________________
//
// $Id: TransferManagerHandlerState.java,v 1.2 2006-07-27 02:57:46 litvinse Exp $
// $Author: litvinse $
//
// created 05/06 by Dmitry Litvintsev (litvinse@fnal.gov)
//
//______________________________________________________________________________


package diskCacheV111.services;


public class TransferManagerHandlerState {
	private int     state;
	private long    transferid;
	private long    transitionTime;
	private boolean causedByError;
	private String  description;

	private TransferManagerHandlerState() {
	}

	public TransferManagerHandlerState(TransferManagerHandler handler, Object errorObject) {

		transferid     = handler.getId();
		state          = handler.getState();
		transitionTime = System.currentTimeMillis();
		switch (state) {
		case TransferManagerHandler.INITIAL_STATE:
			description="INITIAL_STATE";
			causedByError=false;
			break;
		case TransferManagerHandler.WAITING_FOR_PNFS_INFO_STATE:
			description="WAITING_FOR_PNFS_INFO_STATE";
			causedByError=false;
			break;
		case TransferManagerHandler.RECEIVED_PNFS_INFO_STATE:
			description="RECEIVED_PNFS_INFO_STATE";
			causedByError=false;
			break;
		case TransferManagerHandler.WAITING_FOR_PNFS_ENTRY_CREATION_INFO_STATE:
			description="WAITING_FOR_PNFS_ENTRY_CREATION_INFO_STATE";
			causedByError=false;
			break;
		case TransferManagerHandler.RECEIVED_PNFS_ENTRY_CREATION_INFO_STATE:
			description="RECEIVED_PNFS_ENTRY_CREATION_INFO_STATE";
			causedByError=false;
			break;
		case TransferManagerHandler.WAITING_FOR_POOL_INFO_STATE:
			description="WAITING_FOR_POOL_INFO_STATE";
			causedByError=false;
			break;
		case TransferManagerHandler.RECEIVED_POOL_INFO_STATE:
			description="RECEIVED_POOL_INFO_STATE";
			causedByError=false;
			break;
		case TransferManagerHandler.WAITING_FIRST_POOL_REPLY_STATE:
			description="WAITING_FIRST_POOL_REPLY_STATE";
			causedByError=false;
			break;
		case TransferManagerHandler.RECEIVED_FIRST_POOL_REPLY_STATE:
			description="RECEIVED_FIRST_POOL_REPLY_STATE";
			causedByError=false;
			break;
		case TransferManagerHandler.WAITING_FOR_SPACE_INFO_STATE:
			description="WAITING_FOR_SPACE_INFO_STATE";
			causedByError=false;
			break;
		case TransferManagerHandler.RECEIVED_SPACE_INFO_STATE:
			description="RECEIVED_SPACE_INFO_STATE";
			causedByError=false;
			break;
		case TransferManagerHandler.SENT_ERROR_REPLY_STATE:
			description="SENT_ERROR_REPLY_STATE";
			if ( errorObject != null ) {
				description += "(" + errorObject + ")";
			}
			causedByError=true;
			break;
		case TransferManagerHandler.WAITING_FOR_PNFS_ENTRY_DELETE:
			description="WAITING_FOR_PNFS_ENTRY_DELETE";
			causedByError=true;
			break;
		case TransferManagerHandler.RECEIVED_PNFS_ENTRY_DELETE:
			description="RECEIVED_PNFS_ENTRY_DELETE";
			causedByError=true;
			break;
		case TransferManagerHandler.SENT_SUCCESS_REPLY_STATE:
			description="SENT_SUCCESS_REPLY_STATE";
			causedByError=false;
			break;
		default:
			description="";
			causedByError=false;
			break;
		}
	}
}
