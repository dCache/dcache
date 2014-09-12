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
package org.dcache.alarms.jdom;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.jdom.Document;
import org.jdom.Element;
import org.jdom.JDOMException;
import org.jdom.input.SAXBuilder;
import org.jdom.output.Format;
import org.jdom.output.XMLOutputter;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.dcache.alarms.AlarmDefinition;
import org.dcache.alarms.AlarmDefinitionsMap;

/**
 * This implementation maintains current settings via the JDOM
 * implementation of the alarm definition, and writes and reads these from a
 * simple XML file. Definitions can be changed either directly in that file
 * (and then by reloading), or through the add command.
 *
 * @author arossi
 */
public final class XmlBackedAlarmDefinitionsMap
                implements AlarmDefinitionsMap<JDomAlarmDefinition>{

    private final Map<String, JDomAlarmDefinition> internalMap
        = new ConcurrentHashMap<>();

    private String xmlDefinitionsPath;

    public void add(JDomAlarmDefinition definition) {
        internalMap.put(definition.getType(), definition);
    }

    public JDomAlarmDefinition getDefinition(String alarmType)
                    throws NoSuchElementException {
        JDomAlarmDefinition definition = internalMap.get(alarmType);
        if (definition == null) {
            throw new NoSuchElementException(alarmType + " is not defined.");
        }
        return definition;
    }

    public Collection<JDomAlarmDefinition> getDefinitions() {
        return new ArrayList(internalMap.values());
    }

    public void getSortedList(Writer writer) throws Exception {
        XMLOutputter outputter = new XMLOutputter();
        outputter.setFormat(Format.getPrettyFormat());
        outputter.output(getDocument(), writer);
    }

    public Set<String> getTypes() {
        return new HashSet(internalMap.keySet());
    }

    public void initialize() throws JDOMException, IOException {
        load(new Properties());
    }

    public void load(Properties env) throws JDOMException, IOException {
        internalMap.clear();

        File xmlDefinitions
            = new File(env.getProperty(AlarmDefinitionsMap.PATH,
                                       xmlDefinitionsPath));

        if (!xmlDefinitions.exists()) {
                return;
        }

        SAXBuilder builder = new SAXBuilder();
        Document document = builder.build(xmlDefinitions);
        Element rootNode = document.getRootElement();
        List<Element> list = rootNode.getChildren(AlarmDefinition.ALARM_TAG);
        if (!list.isEmpty()) {
            for (Element node : list) {
                add(new JDomAlarmDefinition(node));
            }
        }
    }

    public JDomAlarmDefinition removeDefinition(String alarmType) {
        return internalMap.remove(alarmType);
    }

    public void save(Properties env) throws Exception {
        File xmlDefinitions
            = new File(env.getProperty(AlarmDefinitionsMap.PATH,
                                       xmlDefinitionsPath));

        if (xmlDefinitions.exists()) {
            xmlDefinitions.delete();
        }

        XMLOutputter outputter = new XMLOutputter();
        outputter.setFormat(Format.getPrettyFormat());
        try(FileOutputStream stream
                            = new FileOutputStream(xmlDefinitions, false)){
            outputter.output(getDocument(), stream);
        }
    }

    public void setDefinitionsPath(String xmlDefinitionsPath) {
        Preconditions.checkNotNull(Strings.emptyToNull(xmlDefinitionsPath));
        this.xmlDefinitionsPath = xmlDefinitionsPath;
    }

    private Document getDocument() throws JDOMException {
        Document document = new Document();
        Element root = new Element(JDomAlarmDefinition.ROOT_NAME);
        document.setRootElement(root);
        String[] keys = internalMap.keySet().toArray(new String[internalMap.size()]);
        Arrays.sort(keys);
        for (String key: keys) {
            root.addContent(internalMap.get(key).toElement());
        }
        return document;
    }
}
