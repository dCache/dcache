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
package org.dcache.alarms.commandline;

import org.jdom.Document;
import org.jdom.Element;
import org.jdom.JDOMException;
import org.jdom.input.SAXBuilder;
import org.jdom.output.Format;
import org.jdom.output.XMLOutputter;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Iterator;
import java.util.List;

import org.dcache.alarms.AlarmDefinitionValidationException;
import org.dcache.alarms.logback.AlarmDefinition;

/**
 * Allows the user to add, modify or delete an alarm definition. <br>
 * <br>
 *
 * This command is interactive for ease of entry.
 *
 * @author arossi
 */
public class AlarmDefinitionManager {
    public static final String ADD_CMD = "add";
    public static final String MODIFY_CMD = "modify";
    public static final String REMOVE_CMD = "remove";

    private static final String PROMPT = ">>  ";
    private static final String ERROR_PROMPT = "--> cause: ";
    private static final String MAIN_MSG
        = "Choose attribute to define, 'h[elp]' to describe attributes, "
        + "'q[uit]' to abort, return to process the definition.";
    private static final String ABORT_MSG = "quitting";
    private static final String REMOVE_MSG = "Alarm type to remove:";
    private static final String MODIFY_MSG = "Alarm type to modify:";
    private static final String NO_SUCH_ELEMENT_MSG = "No such alarm definition";
    private static final String INVALID_MSG = "incomplete or invalid definition";

    public static void main(String[] args) {
        try {
            if (args.length < 2) {
                throw new IllegalArgumentException("requires two arguments: ["
                                + ADD_CMD + ", " + MODIFY_CMD + ", "
                                + REMOVE_CMD + "] [path to xml]");
            }

            BufferedReader reader
                = new BufferedReader(new InputStreamReader(System.in));
            File xmlFile = getXmlFile(args[1]);
            Element root = getRootElement(xmlFile);
            XMLOutputter outputter = new XMLOutputter();
            outputter.setFormat(Format.getPrettyFormat());

            String input;

            /**
             * Main switch between add, modify and remove
             */
            switch(args[0]) {
                case ADD_CMD:
                    configure(new AlarmDefinition(), reader, outputter, root,
                                    xmlFile);
                    break;
                case MODIFY_CMD:
                    input = getInput(MODIFY_MSG, reader);
                    if (input == null) {
                        break;
                    }
                    Element toUpdate = remove(input, root);
                    if (toUpdate == null) {
                        System.out.println(NO_SUCH_ELEMENT_MSG);
                        if (proceedWith("Add", reader)) {
                            configure(new AlarmDefinition(), reader, outputter,
                                            root, xmlFile);
                        } else {
                            System.out.println(ABORT_MSG);
                        }
                    } else {
                        printDefinition(toUpdate, outputter);
                        configure(new AlarmDefinition(toUpdate), reader,
                                        outputter, root, xmlFile);
                    }
                    break;
                case REMOVE_CMD:
                    input = getInput(REMOVE_MSG, reader);
                    if (input == null) {
                        break;
                    }
                    Element toRemove = remove(input, root);
                    if (toRemove == null) {
                        System.out.println(NO_SUCH_ELEMENT_MSG);
                        System.out.println(ABORT_MSG);
                    } else {
                        printDefinition(toRemove, outputter);
                        if (proceedWith("OK to delete", reader)) {
                            update(outputter, root, xmlFile);
                        } else {
                            System.out.println(ABORT_MSG);
                        }
                    }
                    break;
            }
        } catch (Throwable e) {
            printError(e);
            System.exit(1);
        }
    }

    static void printError(Throwable t) {
        if (t != null) {
            if (t instanceof NullPointerException) {
                /*
                 * a bug, shouldn't happen
                 */
                t.printStackTrace();
            } else {
                System.err.println(t.getMessage());
            }
            t = t.getCause();
        }
        while (t != null) {
            System.err.println(ERROR_PROMPT + t.getMessage());
            t = t.getCause();
        }
    }

    /**
     * Upon successful completion of choices, will ask to proceed with write.
     */
    private static void configure(AlarmDefinition definition,
                                  BufferedReader reader,
                                  XMLOutputter outputter,
                                  Element rootNode,
                                  File xmlFile) throws Exception {
        while (true) {
            definition = getDefinition(definition, reader);
            if (definition == null) {
                return;
            }

            try {
                definition.validate();

                Element alarmType = definition.toElement();
                printDefinition(alarmType, outputter);

                if (proceedWith("Add/Update definition", reader)) {
                    update(alarmType, outputter, rootNode, xmlFile);
                    break;
                }
                System.out.println("Quit? <q>:");
                if (reader.readLine().equalsIgnoreCase("q")) {
                    break;
                }
            } catch (AlarmDefinitionValidationException e) {
                System.out.println(e.getMessage());
                System.out.println(INVALID_MSG);
            }
        }
    }

    /**
     * Will iterate over the attribute choices until it receives a blank
     * line return.
     */
    private static AlarmDefinition getDefinition(AlarmDefinition definition,
                    BufferedReader reader)
                    throws IOException {
        while (true) {
            System.out.println(MAIN_MSG);
            String input = getInput("Attributes: "
                            + AlarmDefinition.ATTRIBUTES, reader);
            if (input == null) {
                break;
            } else {
                input = input.trim();
            }

            switch (input.toLowerCase()) {
                case "h":
                case "help":
                    System.out.println(AlarmDefinition.getAttributesDescription());
                    break;
                case "q":
                case "quit":
                    System.out.println(ABORT_MSG);
                    return null;
                default:
                    verifyInput(input, definition, reader);
            }
        }

        return definition;
    }

    private static String getInput(String message, BufferedReader reader)
                    throws IOException {
        System.out.println(message);
        System.out.print(PROMPT);
        String input = reader.readLine();
        if (input.isEmpty()) {
            return null;
        }
        return input;
    }

    private static Element getRootElement(File xmlFile)
                    throws JDOMException, IOException {
        SAXBuilder builder = new SAXBuilder();
        Document document = builder.build(xmlFile);
        return document.getRootElement();
    }

    private static File getXmlFile(String path) throws IOException {
        File xmlFile = new File(path);
        if (!xmlFile.exists()) {
            throw new FileNotFoundException("not found: "
                            + xmlFile.getAbsolutePath());
        } else if (!xmlFile.isFile()) {
            throw new IOException("not a file: " + xmlFile.getAbsolutePath());
        }
        return xmlFile;
    }

    private static void printDefinition(Element alarmType, XMLOutputter outputter)
                    throws IOException {
        System.out.println("Alarm Definition:");
        System.out.println("----------------------------------");
        outputter.output(alarmType, System.out);
        System.out.println();
        System.out.println("----------------------------------");
    }

    private static boolean proceedWith(String question, BufferedReader reader)
                    throws IOException {
        System.out.print(question);
        System.out.println("? <y/n> [n]:");
        String input = reader.readLine();
        return input.trim().equalsIgnoreCase("y");
    }

    private static Element remove(String type, Element rootNode) {
        List children = rootNode.getChildren();
        for (Iterator i = children.iterator(); i.hasNext();) {
            Element alarmType = (Element)i.next();
            if (type.equals(alarmType.getChild("type").getTextTrim())) {
                i.remove();
                return alarmType;
            }
        }
        return null;
    }

    /**
     * Removes any definition of the same type first, then clones the children
     * into a new root element, adding the new AlarmDefinition.
     */
    private static void update(Element definition,
                              XMLOutputter outputter,
                              Element rootNode,
                              File xmlFile) throws JDOMException, IOException {
        String type = definition.getChild("type").getTextTrim();
        remove(type, rootNode);
        Element newRoot = new Element("definitions");
        newRoot.addContent(rootNode.cloneContent());
        newRoot.addContent(definition);
        writeToFile(newRoot, outputter, xmlFile);
    }

    /**
     * Clones the children into a new root element.
     */
    private static void update(XMLOutputter outputter,
                    Element rootNode,
                    File xmlFile) throws JDOMException, IOException {
        Element newRoot = new Element("definitions");
        newRoot.addContent(rootNode.cloneContent());
        writeToFile(newRoot, outputter, xmlFile);
    }

    /**
     * Loops for a particular attribute if input does not validate.
     */
    private static void verifyInput(String option, AlarmDefinition definition,
                    BufferedReader reader) throws IOException {
        try {
            System.out.println("("
                                + AlarmDefinition.getAttributeDescription(option)
                                + ")");
        } catch (AlarmDefinitionValidationException e) {
            System.out.println(e.getMessage());
            return;
        }

        while (true) {
            try {
                String input = getInput("hit return to skip, "
                                + AlarmDefinition.RM
                                + " to remove value", reader);
                if (input == null) {
                    return;
                }
                definition.validateAndSet(option, input);
                break;
            } catch (AlarmDefinitionValidationException e) {
                System.out.println(e.getMessage());
            }
        }
    }

    /**
     * Creates new document, sets root and writes to file without append.
     */
    private static void writeToFile(Element newRoot, XMLOutputter outputter,
                    File xmlFile) throws IOException {
        Document newDocument = new Document();
        newDocument.setRootElement(newRoot);
        try(FileOutputStream stream = new FileOutputStream(xmlFile, false)){
            outputter.output(newDocument, stream);
        }
    }
}
