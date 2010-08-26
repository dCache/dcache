// $Id$
// $Log: not supported by cvs2svn $
// Revision 1.7  2006/07/04 22:23:37  timur
// Use Credential Id to reffer to the remote credential in delegation step, reformated some classes
//
// Revision 1.6  2005/03/11 21:17:28  timur
// making srm compatible with cern tools again
//
// Revision 1.5  2005/01/25 05:17:31  timur
// moving general srm stuff into srm repository
//
// Revision 1.4  2004/11/08 23:02:40  timur
// remote gridftp manager kills the mover when the mover thread is killed,  further modified the srm database handling
//
// Revision 1.3  2004/08/26 21:22:30  timur
// scheduler bug (not setting job value to null) in a loop serching for the next job to execute
//
// Revision 1.2  2004/08/06 19:35:23  timur
// merging branch srm-branch-12_May_2004 into the trunk
//
// Revision 1.1.2.2  2004/06/15 22:15:42  timur
// added cvs logging tags and fermi copyright headers at the top
//

/*
COPYRIGHT STATUS:
  Dec 1st 2001, Fermi National Accelerator Laboratory (FNAL) documents and
  software are sponsored by the U.S. Department of Energy under Contract No.
  DE-AC02-76CH03000. Therefore, the U.S. Government retains a  world-wide
  non-exclusive, royalty-free license to publish or reproduce these documents
  and software for U.S. Government purposes.  All documents and software
  available from this server are protected under the U.S. and Foreign
  Copyright Laws, and FNAL reserves all rights.
 
 
 Distribution of the software available from this server is free of
 charge subject to the user following the terms of the Fermitools
 Software Legal Information.
 
 Redistribution and/or modification of the software shall be accompanied
 by the Fermitools Software Legal Information  (including the copyright
 notice).
 
 The user is asked to feed back problems, benefits, and/or suggestions
 about the software to the Fermilab Software Providers.
 
 
 Neither the name of Fermilab, the  URA, nor the names of the contributors
 may be used to endorse or promote products derived from this software
 without specific prior written permission.
 
 
 
  DISCLAIMER OF LIABILITY (BSD):
 
  THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
  "AS IS" AND ANY EXPRESS OR IMPLIED  WARRANTIES, INCLUDING, BUT NOT
  LIMITED TO, THE IMPLIED  WARRANTIES OF MERCHANTABILITY AND FITNESS
  FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL FERMILAB,
  OR THE URA, OR THE U.S. DEPARTMENT of ENERGY, OR CONTRIBUTORS BE LIABLE
  FOR  ANY  DIRECT, INDIRECT,  INCIDENTAL, SPECIAL, EXEMPLARY, OR
  CONSEQUENTIAL DAMAGES  (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT
  OF SUBSTITUTE  GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR
  BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY  OF
  LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
  NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT  OF THE USE OF THIS
  SOFTWARE, EVEN IF ADVISED OF THE  POSSIBILITY OF SUCH DAMAGE.
 
 
  Liabilities of the Government:
 
  This software is provided by URA, independent from its Prime Contract
  with the U.S. Department of Energy. URA is acting independently from
  the Government and in its own private capacity and is not acting on
  behalf of the U.S. Government, nor as its contractor nor its agent.
  Correspondingly, it is understood and agreed that the U.S. Government
  has no connection to this software and in no manner whatsoever shall
  be liable for nor assume any responsibility or obligation for any claim,
  cost, or damages arising out of or resulting from the use of the software
  available from this server.
 
 
  Export Control:
 
  All documents and software available from this server are subject to U.S.
  export control laws.  Anyone downloading information from this server is
  obligated to secure any necessary Government licenses before exporting
  documents or software obtained from this server.
 */

/*
 * StageAndPinCompanion.java
 *
 * Created on January 2, 2003, 2:08 PM
 */

package diskCacheV111.srm.dcache;

import dmg.cells.nucleus.CellAdapter;
import dmg.cells.nucleus.CellPath;
import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellMessageAnswerable;
import diskCacheV111.util.PnfsId;
//import diskCacheV111.vehicles.PoolSetStickyMessage;
import diskCacheV111.vehicles.PinManagerUnpinMessage;
import diskCacheV111.vehicles.Message;
import org.dcache.auth.AuthorizationRecord;
import org.dcache.srm.UnpinCallbacks;

/**
 *
 * @author  timur
 */
/**
 * this class does all the dcache specific work needed for staging and pinning a
 * file represented by a path. It notifies the caller about each next stage
 * of the process via a StageAndPinCompanionCallbacks interface.
 * Boolean functions of the callback interface need to return true in order for
 * the process to continue
 */
public class UnpinCompanion implements CellMessageAnswerable {
    
    private static final int INITIAL_STATE=0;
    private static final int SENT_PIN_MGR_PIN_MSG = 5;
    private static final int RECEIVED_PIN_MGR_PIN_MSG = 6;
    private volatile int  state = INITIAL_STATE;
    private dmg.cells.nucleus.CellAdapter cell;
    private UnpinCallbacks callbacks;
    private CellMessage request = null;
    private String fileId;
    private AuthorizationRecord user;
    
    private void say(String words_of_wisdom) {
        if(cell!=null) {
            cell.say(" UnpinCompanion: "+words_of_wisdom);
        }
    }
    
    private void esay(String words_of_despare) {
        if(cell!=null) {
            cell.esay(" UnpinCompanion : "+words_of_despare);
        }
    }
    /** Creates a new instance of StageAndPinCompanion */
    
    private UnpinCompanion(AuthorizationRecord user, String fileId, UnpinCallbacks callbacks, CellAdapter cell) {
        this.user = user;
        this.fileId = fileId;
        this.cell = cell;
        this.callbacks = callbacks;
        say(" constructor ");
    }
    
    public void answerArrived( final CellMessage req , final CellMessage answer ) {
        say("answerArrived");
        diskCacheV111.util.ThreadManager.execute(new Runnable() {
            public void run() {
                processMessage(req,answer);
            }
        });
    }
    
    private void processMessage( CellMessage req , CellMessage answer ) {
        say("answerArrived");
        request = req;
        Object o = answer.getMessageObject();
        if(o instanceof Message) {
            Message message = (Message)answer.getMessageObject() ;
            if( message instanceof PinManagerUnpinMessage ) {
                if(state != SENT_PIN_MGR_PIN_MSG) {
                    esay("received PinManagerUnpinMessage, state ="+state);
                    return;
                }
                state = RECEIVED_PIN_MGR_PIN_MSG;
                PinManagerUnpinMessage unpinResponse =
                        (PinManagerUnpinMessage)message;
                pinManagerUnpinMessageArrived(unpinResponse);
            } else {
                esay(this.toString()+" got unknown message "+
                        " : "+message.getErrorObject());
                
                callbacks.Error( this.toString()+" got unknown message "+
                        " : "+message.getErrorObject()) ;
            }
        } else {
            esay(this.toString()+" got unknown object "+
                    " : "+o);
            callbacks.Error(this.toString()+" got unknown object "+
                    " : "+o) ;
        }
    }
    
    
    private void pinManagerUnpinMessageArrived(PinManagerUnpinMessage unpinResponse) {
        say(" message is PinManagerUnpinMessage");
        if(unpinResponse.getReturnCode() != 0) {
            esay("UnpinRequest Failed");
            callbacks.UnpinningFailed(unpinResponse.getErrorObject().toString());
            return ;
        }
        say("unpinned");
        callbacks.Unpinned(unpinResponse.getPinRequestId());
    }
    
    public void exceptionArrived( CellMessage request , Exception exception ) {
        esay("exceptionArrived "+exception+" for request "+request);
        callbacks.Exception(exception);
    }
    public void answerTimedOut( CellMessage request ) {
        esay("answerTimedOut for request "+request);
        callbacks.Timeout();
    }
    public String toString() {
        
        return this.getClass().getName()+" "+
                fileId;
    }
    
    public static void unpinFile(
            AuthorizationRecord user,
            String fileId,
            String pinId,
            UnpinCallbacks callbacks,
            CellAdapter cell) {
        cell.say("UnpinCompanion.unpinFile("+fileId+")");
        PnfsId pnfsId = null;
        try {
            pnfsId = new PnfsId(fileId);
        } catch(Exception e) {
            //just quietly fail, if the fileId is not pnfs id
            // it must be created by a disk srm during testing
            //just allow the request to get expired
            callbacks.Unpinned(fileId);
            return;
        }
        
        UnpinCompanion companion = new UnpinCompanion(user,fileId,
                callbacks,cell);
        
        PinManagerUnpinMessage unpinRequest =
                new PinManagerUnpinMessage( pnfsId, pinId);
        unpinRequest.setAuthorizationRecord(user);
        
        companion.state = SENT_PIN_MGR_PIN_MSG;
        try {
            cell.sendMessage(
                    new CellMessage(
                    new CellPath( "PinManager") ,
                    unpinRequest ) ,
                    true , true,
                    companion,
                    60*60*1000
                    );
            //say("StageAndPinCompanion: recordAsPinned");
            //rr.recordAsPinned (_fr,true);
        }catch(Exception ee ) {
            cell.esay(ee);
            callbacks.UnpinningFailed(ee.toString());
            return ;
        }
    }
    
    public static void unpinFileBySrmRequestId(
            AuthorizationRecord user,
            String fileId,
            long  srmRequestId,
            UnpinCallbacks callbacks,
            CellAdapter cell) {
        cell.say("UnpinCompanion.unpinFile("+fileId+")");
        PnfsId pnfsId = null;
        try {
            pnfsId = new PnfsId(fileId);
        } catch(Exception e) {
            //just ciletly fail, if the fileId is not pnfs id
            // it must be created by a disk srm during testing
            //just allow the request to get expired
            callbacks.Unpinned(fileId);
            return;
        }
        
        UnpinCompanion companion = new UnpinCompanion(user,fileId,
                callbacks,cell);
        
        PinManagerUnpinMessage unpinRequest =
                new PinManagerUnpinMessage( pnfsId, srmRequestId);
        unpinRequest.setAuthorizationRecord(user);
        companion.state = SENT_PIN_MGR_PIN_MSG;
        try {
            cell.sendMessage(
                    new CellMessage(
                    new CellPath( "PinManager") ,
                    unpinRequest ) ,
                    true , true,
                    companion,
                    60*60*1000
                    );
            //say("StageAndPinCompanion: recordAsPinned");
            //rr.recordAsPinned (_fr,true);
        }catch(Exception ee ) {
            cell.esay(ee);
            callbacks.UnpinningFailed(ee.toString());
            return ;
        }
    }
    public static void unpinFile(
            AuthorizationRecord user,
            String fileId,
            UnpinCallbacks callbacks,
            CellAdapter cell) {
        cell.say("UnpinCompanion.unpinFile("+fileId+")");
        PnfsId pnfsId = null;
        try {
            pnfsId = new PnfsId(fileId);
        } catch(Exception e) {
            //just ciletly fail, if the fileId is not pnfs id
            // it must be created by a disk srm during testing
            //just allow the request to get expired
            callbacks.Unpinned(fileId);
            return;
        }
        
        UnpinCompanion companion = new UnpinCompanion(user,fileId,
                callbacks,cell);
        
        PinManagerUnpinMessage unpinRequest =
                new PinManagerUnpinMessage( pnfsId);
        unpinRequest.setAuthorizationRecord(user);
        companion.state = SENT_PIN_MGR_PIN_MSG;
        try {
            cell.sendMessage(
                    new CellMessage(
                    new CellPath( "PinManager") ,
                    unpinRequest ) ,
                    true , true,
                    companion,
                    60*60*1000
                    );
            //say("StageAndPinCompanion: recordAsPinned");
            //rr.recordAsPinned (_fr,true);
        }catch(Exception ee ) {
            cell.esay(ee);
            callbacks.UnpinningFailed(ee.toString());
            return ;
        }
    }
}

