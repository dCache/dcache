// $Id: UpdateSpaceCompanion.java,v 1.1 2005-11-01 23:28:13 timur Exp $
// $Log: not supported by cvs2svn $
// Revision 1.11  2005/10/07 22:59:47  timur
// work towards v2
//
// Revision 1.10  2005/09/30 21:47:39  timur
// more space reservation - pnfs communication improvements
//
// Revision 1.9  2005/09/28 21:36:07  timur
// removed unused file, more debugging when deleting pnfs entry
//
// Revision 1.8  2005/09/27 21:46:51  timur
// do not leave pnfs entry behind after space reservation is created
//
// Revision 1.7  2005/03/10 23:12:07  timur
// Fisrt working version of space reservation module
//
// Revision 1.6  2005/03/09 23:22:57  timur
// more space reservation code
//
// Revision 1.5  2005/03/07 22:57:44  timur
// more work on space reservation
//
// Revision 1.4  2005/02/17 02:16:30  timur
//  added a debug message
//
// Revision 1.3  2005/02/02 22:57:21  timur
// working on space manager
//
// Revision 1.2  2005/01/31 22:52:04  timur
// started working on space reservation
//
// Revision 1.1  2004/10/20 21:32:30  timur
// adding classes for space management
//
// Revision 1.2  2004/08/06 19:35:23  timur
// merging branch srm-branch-12_May_2004 into the trunk
//
// Revision 1.1.2.2  2004/06/15 22:15:42  timur
// added cvs logging tags and fermi copyright headers at the top
//
// Revision 1.1.2.1  2004/05/18 21:40:30  timur
// incorporation of the new scheduler into srm, repackaging of all the srm classes
//
// Revision 1.1  2003/10/30 00:15:57  cvs
// srmReserveSpace implemented, started working on srmPrepareToPut
//
// Revision 1.6  2003/10/02 18:50:28  cvs
// timur: added cvs version and log in comments
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

package diskCacheV111.services;

import dmg.cells.nucleus.CellAdapter;
import dmg.cells.nucleus.CellPath;
import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellMessageAnswerable;

import diskCacheV111.util.PnfsId;

import diskCacheV111.vehicles.PoolReserveSpaceMessage;
import diskCacheV111.vehicles.PoolQuerySpaceReservationMessage;
import diskCacheV111.vehicles.PoolFreeSpaceReservationMessage;
import diskCacheV111.vehicles.Message;
import java.net.InetAddress;

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
public class UpdateSpaceCompanion implements CellMessageAnswerable {
    private  static final int NOT_WAITING_STATE=0;
    private  static final int WAITING_POOL_QUERY_RESPONSE_STATE=1;
    private  static final int WAITING_POOL_RESERVE_RESPONSE_STATE=2;
    private  static final int WAITING_POOL_RELEASE_RESPONSE_STATE=3;
    private volatile int state = NOT_WAITING_STATE;
    private dmg.cells.nucleus.CellAdapter cell;
    private String pool;
    private long spaceSize;
    private void say(String words_of_wisdom) {
        if(cell!=null) {
            cell.say(" UpdateSpaceCompanion["+pool+","+spaceSize+"] : "+words_of_wisdom);
        }
    }

    private void esay(String words_of_despare) {
        if(cell!=null) {
            cell.esay(" UpdateSpaceCompanion["+pool+","+spaceSize+"] : "+words_of_despare);
        }
    }
    private void esay(Throwable t) {
        if(cell!=null) {
            cell.esay(" UpdateSpaceCompanion exception["+pool+","+spaceSize+"] : ");
            cell.esay(t);
        }
    }

    public static final String getStateString(int state) {
        switch(state) {
            case NOT_WAITING_STATE:
                return "NOT_WAITING_STATE";
            case WAITING_POOL_QUERY_RESPONSE_STATE:
                return "WAITING_POOL_QUERY_RESPONSE_STATE";
            case WAITING_POOL_RESERVE_RESPONSE_STATE:
                return "WAITING_POOL_RESERVE_RESPONSE_STATE";
            case WAITING_POOL_RELEASE_RESPONSE_STATE:
                return "WAITING_POOL_RELEASE_RESPONSE_STATE";
            default:
                return "UNKNOWN";
        }
    }


    /** Creates a new instance of StageAndPinCompanion */

    private UpdateSpaceCompanion(String pool, long spaceSize, CellAdapter cell) {
        this.pool = pool;
        this.cell = cell;
        this.spaceSize = spaceSize;
    }



    public void answerArrived( CellMessage req , CellMessage answer ) {
        int current_state = state;
        say("answerArrived, state="+getStateString(current_state));
        Object o = answer.getMessageObject();
        if(o instanceof Message) {
            Message message = (Message)answer.getMessageObject() ;
            if(message instanceof PoolQuerySpaceReservationMessage  &&
                current_state == WAITING_POOL_QUERY_RESPONSE_STATE) {
                state=NOT_WAITING_STATE;
                PoolQuerySpaceReservationMessage msg =
                (PoolQuerySpaceReservationMessage)message;
               queryResponseArrived(msg);
            }
            else if(message instanceof PoolReserveSpaceMessage  &&
                current_state == WAITING_POOL_RESERVE_RESPONSE_STATE) {
                say("researve answer arrived "+message);
                if(message.getReturnCode() != 0) {
                    esay("researve failed"+message.getErrorObject());
                }
            }
            else if(message instanceof PoolFreeSpaceReservationMessage  &&
                current_state == WAITING_POOL_RELEASE_RESPONSE_STATE) {
                say("release answer arrived "+message);
                if(message.getReturnCode() != 0) {
                    esay("release failed"+message.getErrorObject());
                }
            }
            else {
                esay("ignoring unexpected message : "+message);
                //callbacks.ReserveSpaceFailed("unexpected message arrived:"+message);
                return ;
            }
        }
        else {
            esay(" got unknown object. ignoring "+
            " : "+o);
            //callbacks.Error(this.toString ()+" got unknown object "+
            //" : "+o) ;
        }
    }

    public void exceptionArrived( CellMessage request , Exception exception ) {
        esay("exceptionArrived "+exception+" for request "+request);

    }
    public void answerTimedOut( CellMessage request ) {
        esay("answerTimedOut for request "+request);
    }
    public String toString() {

        return this.getClass().getName()+
        pool+":"+spaceSize;
    }



    public  void reserveSpace(
    long reserveSize)
    {
        say("reserveSpace("+reserveSize+")");
        PoolReserveSpaceMessage reserveRequest =
        new PoolReserveSpaceMessage(pool,reserveSize);
        try {
            state = WAITING_POOL_RESERVE_RESPONSE_STATE;
            cell.sendMessage( new CellMessage(
            new CellPath(pool) ,
            reserveRequest ) ,
            true , true ,
            this ,
            1*24*60*60*1000) ;
        }
        catch(Exception ee ) {
            esay(ee);
        }

    }

    public  void releaseSpace(
    long releaseSize)
    {
        say("releaseSpace("+releaseSize+")");
        PoolFreeSpaceReservationMessage releaseRequest =
        new PoolFreeSpaceReservationMessage(pool,releaseSize);
        try {
            state = WAITING_POOL_RELEASE_RESPONSE_STATE;
            cell.sendMessage( new CellMessage(
            new CellPath(pool) ,
            releaseRequest ) ,
            true , true ,
            this ,
            1*24*60*60*1000) ;
        }
        catch(Exception ee ) {
            esay(ee);
        }

    }

    public void queryPoolReservedSpace(){
        PoolQuerySpaceReservationMessage queryRequest =
        new PoolQuerySpaceReservationMessage(pool);
        try {
            state = WAITING_POOL_QUERY_RESPONSE_STATE;
            cell.sendMessage( new CellMessage(
            new CellPath(pool) ,
            queryRequest ) ,
            true , true ,
            this ,
            1*24*60*60*1000) ;
        }
        catch(Exception ee ) {
            esay(ee);
        }

    }

    public void queryResponseArrived(PoolQuerySpaceReservationMessage queryResponse) {
     long poolReservedSpace = queryResponse.getReservedSpace();
     if(poolReservedSpace >spaceSize) {
         releaseSpace(poolReservedSpace - spaceSize);
     }
     else if(poolReservedSpace <spaceSize) {
         reserveSpace(spaceSize-poolReservedSpace);
     }
     else
     {
         say("pool space size is exactly what it should be");
     }
    }

    public static void updateSpaceAmount(
    String pool,
    long spaceSize,
    CellAdapter cell) {
        cell.say(" UpdateSpaceCompanion.updateSpaceAmount("+pool+",size="+spaceSize+")");

        UpdateSpaceCompanion companion =
        new UpdateSpaceCompanion(
        pool,
        spaceSize,cell);
        companion.queryPoolReservedSpace();
    }


}

