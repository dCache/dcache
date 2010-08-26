package org.dcache.services.infoCollector;

import java.net.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.io.*;
import dmg.util.Args ;

/**
 * Information Exporter class.<br>
 * This class is instantiated by the <code>InfoCollector</code> to send
 * over a plain tcp socket a <code>Schema</code> object that carries out
 * dynamic information from dCache.<br><br>
 * Also this class is independent from the particular implementation of
 * Schema used. As matter of fact, this class serializes a generic Schema
 * object over a socket. It's a job of the client to know what particular
 * implementation of Schema was sent.<br><br>
 * Note that client needs only to know the specializing class of the Schema.
 */
public class InfoExporter implements Runnable {

	/** InfoCollector link, to call say() **/
	private final InfoCollector _cell;

	/** The SchemaFiller used **/
	private final SchemaFiller _filler;

	/** The Server Thread **/
	private final Thread   _sendThread ;

	/** Flag for strict-loop enabling **/
	private AtomicBoolean  _continue     = new AtomicBoolean( true );

	/** TCP port that the server listen **/
	public  int _port = 22111;

	/** Server Socket reference**/
	private final ServerSocket _server;

	/** Constructor.<br>
	 * Starts the thread and the server.
	 * @param filler SchemaFiller created in the InfoCollector
	 * @param cell InfoCollector reference
	 * @throws Exception
	 **/
	public InfoExporter(SchemaFiller filler, InfoCollector cell) throws Exception{
	   _filler = filler;
        _cell = cell;
        Args opt = _cell.getArgs();
        try {

            String option = opt.getOpt("listenPort");
            if ((option != null) && (option.length() > 0)) {
                try {
                    _port = Integer.parseInt(option);
                } catch (NumberFormatException e) {
                    // ignore bad values
                }
            }
            _cell.say("InfoExporter: Using tcp listen port number : " + _port);

            _server = new ServerSocket(_port);

            _sendThread = new Thread(this);
            _sendThread.start();

        } catch (IOException ie) {
            cell.say("InfoExporter: NOT STARTED!");
            throw ie;
        }

	}


	/**
	 * This method stops the loop and cleans the socket.<br>
	 * It must to be called explicitly when the InfoCollector
	 * is killed to force the socket closure.
	 * If it is not called when InfoCollector is killed, the
	 * port 22111 continues to be binded from the thread and
	 * will not be more possible instanciates correctly a new
	 * InfoCollector cell.
	 */
	public void cleanUp(){
		try{
		      _continue.set(false);
		      _server.close();
		      _cell.say("InfoExporter: Finalized.");
		   }catch(IOException ie){
	              _cell.esay(ie);
		   }
	}


	/**
	 * Finalization: calls the cleaner.
	 */
	protected void finalize(){
		cleanUp();
		_cell.say("InfoExporter: Finalized.");
	}


	/**
	 * Thread:<br>
	 * In a strict-loop the server waits for a connection request.
	 * When the connection arrives, a stream is opened over the socket
	 * and the <code>Schema</code> in the <code>SchemaFiller</code> is
	 * filled with the information actually in the <code>Infobase</code>
	 * and delivered on the stream.
	 */
	public void run() {

        if (Thread.currentThread() == _sendThread) {
            synchronized (this) {
                while (_continue.get()) {

                    try {
                        Socket socket = _server.accept();

                        try {

                            ObjectOutputStream out = new ObjectOutputStream(
                                    socket.getOutputStream());

                            _filler.fillSchema();
                            out.writeObject(_filler.schema);
                            out.flush();

                        } catch (IOException ioe) {
                            _cell.say("InfoExporter: Problems using socket!");
                            _cell.esay(ioe);
                        } finally {
                            socket.close();
                        }

                    } catch (IOException ioe) {
                        _cell.say("InfoExporter: Problems creating socket!");
                        _cell.esay(ioe);
                    }

                }
            }
        }
    }




}
