/*
 * $Id: PnfsManagerBroker.java,v 1.7 2006-01-16 15:04:28 tigran Exp $
 */
package diskCacheV111.namespace;

import diskCacheV111.util.PnfsId;
import diskCacheV111.vehicles.Message;
import diskCacheV111.vehicles.PnfsMessage;
import dmg.cells.nucleus.CellAdapter;
import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellNucleus;
import dmg.cells.nucleus.NoRouteToCellException;
import dmg.cells.nucleus.SyncFifo2;
import dmg.util.Args;
import dmg.cells.nucleus.CellPath;

import java.util.Map;
import java.util.HashMap;
import java.util.Collection;
import java.util.Iterator;
import java.util.Set;
import java.util.StringTokenizer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PnfsManagerBroker extends CellAdapter {

    private final static Logger _log =
        LoggerFactory.getLogger(PnfsManagerBroker.class);

    private String      _cellName;
    private Args        _args;
    private CellNucleus _nucleus;
    private SyncFifo2  _fifo    = new  SyncFifo2();
    private final Map<String,WorkerInstance> _pnfsManagers =
            new HashMap<>();


    public PnfsManagerBroker(String cellName, String args)
    {
        super( cellName , PnfsManagerBroker.class.getName(), args , false ) ;

        _cellName = cellName ;
        _args     = getArgs() ;
        _nucleus  = getNucleus() ;


        _pnfsManagers.put("default", new WorkerInstance("default") );
        _nucleus.newThread( new MessageWizard(_fifo, this) , "MessageWizard").start() ;

        useInterpreter( true ) ;

        //Make the cell name well-known
        getNucleus().export();
        start() ;

    }



    // comands

    public String ac_map_$_1(Args args) {

        String newItem = args.argv(0);
        synchronized(_pnfsManagers) {
            _pnfsManagers.put(newItem, new WorkerInstance(newItem) );
        }
        return "Ok";

    }

    public String ac_dump_$_0(Args args) {

        StringBuilder sb = new StringBuilder();
        synchronized(_pnfsManagers) {
            for (String element: _pnfsManagers.keySet()) {
                sb.append(element).append("\n");
            }
        }
        return sb.toString();

    }


    //////////////////////////////////////////////////////////////////////////



    @Override
    public void messageArrived(CellMessage message) {


        Object pnfsMessage  = message.getMessageObject();
        if (! (pnfsMessage instanceof Message) ){
            _log.info("Unexpected message class "+pnfsMessage.getClass());
            _log.info("source = "+message.getSourceAddress());
            return;
        }

        if( pnfsMessage == null ) {
            _log.info("Null message,  source = "+message.getSourceAddress());
            return;
        }

        if( pnfsMessage instanceof PnfsMessage ) {
            _fifo.push(message);
        }
    }


    private class MessageWizard implements Runnable {

        private SyncFifo2  _fifo;
        private CellAdapter       _cell;
        private boolean _isStopped;

        MessageWizard(SyncFifo2 fifo, CellAdapter cell) {
            _fifo = fifo;
            _cell = cell;
        }

        @Override
        public void run() {

            while( !_isStopped ) {
                try {

                    CellMessage  pnfsMessage = (CellMessage)_fifo.pop();
                    forward(pnfsMessage);

                }catch(Exception e ) {
                    _log.warn(e.getMessage(), e);
                    _isStopped = true;
                }
            }

        }


        private void forward (CellMessage pnfsMessage ) {
            PnfsMessage pnfsMessageObject   = (PnfsMessage)pnfsMessage.getMessageObject() ;
            PnfsId      pnfsId = pnfsMessageObject.getPnfsId() ;
            String      pnfsInstance = null;

            if( pnfsId != null) {
                pnfsInstance = pnfsId.getDomain();
                pnfsMessageObject.setPnfsId( pnfsId );
            }else {
                //request with path!
                // let say, path format is: /pnfs/instance/path
                String path = pnfsMessageObject.getPnfsPath();
                if( path != null ) {
                    StringTokenizer st = new StringTokenizer( path, "[/]");
                    st.nextToken(); // skip pnfs
                    pnfsInstance = st.nextToken();
                }
            }

            if( pnfsInstance == null ) {
                pnfsInstance = "default";
            }

            synchronized(_pnfsManagers) {
                WorkerInstance pManager = (WorkerInstance)_pnfsManagers.get(pnfsInstance);
                new MessageBroker(_cell.getNucleus(), pnfsMessage,
                        new CellPath(_cellName +"-"+pManager.getName()), pnfsInstance ).start();
                pManager.newAction();
            }
        }
    }


    /**
     *
     * @author tigran
     *
     * Helper class - send msg to dest and reply to src
     *
     */

    private class MessageBroker extends Thread {

        private CellPath _destination;
        private CellMessage _message;
        private CellNucleus _nucleus;
        private String _domain;
        private long _timeout = 600 * 1000;

        MessageBroker(CellNucleus nucleus, CellMessage msg, CellPath dest, String domain) {
            _message = msg;
            _destination = dest;
            _nucleus = nucleus;
            _domain = domain;
        }

        @Override
        public void run() {

            try {

                CellMessage msg = new CellMessage(_destination, _message.getMessageObject());
                _log.info("forvarding message to cell " + _destination.getCellName());
                if( !((PnfsMessage)_message.getMessageObject()).getReplyRequired() ) {
                    _nucleus.sendMessage(msg);
                }else{
                    CellMessage reply = _nucleus.sendAndWait(msg, _timeout);
                    _log.info("reply to: " + _message.getSourceAddress() + " mgs=" + reply);

                    Object messageObject = reply.getMessageObject();
                    if( !_domain.equals("default") && (messageObject instanceof PnfsMessage) ) {
                        PnfsId pnfsId = ((PnfsMessage)messageObject).getPnfsId();
                        if( pnfsId != null ) {
                            ((PnfsMessage)messageObject).setPnfsId( new PnfsId( pnfsId.getId(), _domain) );
                        }
                    }

                    _message.setMessageObject(reply.getMessageObject());
                    _message.revertDirection();
                    _nucleus.sendMessage(_message);
                }

            }catch(NoRouteToCellException | InterruptedException nr) {

            } catch(Exception e) {
                _log.warn(e.getMessage(), e);
            }

        }

    }

    /**
     *
     * container class
     *
     */

    private class WorkerInstance {

        String _name;
        long  _actions;

        WorkerInstance(String name) {
            _name = name;
        }


        long getActions() { return _actions; }
        String getName() { return _name; }
        void newAction() { ++_actions; }
    }


    @Override
    public String getInfo() {
        StringBuilder sb = new StringBuilder();

        sb.append("$Id: PnfsManagerBroker.java,v 1.7 2006-01-16 15:04:28 tigran Exp $").append("\n\n");

        synchronized(_pnfsManagers) {
            for (WorkerInstance worker : _pnfsManagers.values()) {
                sb.append(worker.getName()).append(" : ").append(worker.getActions())
                        .append("\n");
            }
        }


        return sb.toString();
    }
}
