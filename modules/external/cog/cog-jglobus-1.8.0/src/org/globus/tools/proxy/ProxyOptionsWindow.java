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
package org.globus.tools.proxy;

import java.awt.Dialog;
import java.awt.BorderLayout;
import java.awt.Container;
import java.awt.event.ActionListener;
import java.awt.event.ActionEvent;
import javax.swing.JDialog;
import javax.swing.JPanel;
import javax.swing.JButton;
import javax.swing.BorderFactory;

import org.globus.common.CoGProperties;

public class ProxyOptionsWindow extends JDialog implements ActionListener {

    private ProxyOptionsPanel optionsPanel;

    private CoGProperties properties;

    private JButton applyBt = null;

    private JButton cancelBt = null;

    public ProxyOptionsWindow(Dialog owner, boolean modal, boolean pkcs11) {

        super(owner, modal);
        setTitle("Options");

        if (pkcs11) {
            optionsPanel = new PKCS11ProxyOptionsPanel();
            optionsPanel.setBorder(BorderFactory
                    .createTitledBorder(" PKCS11 Options "));
        } else {
            optionsPanel = new DefaultProxyOptionsPanel();
            optionsPanel.setBorder(BorderFactory
                    .createTitledBorder(" Proxy Options "));
        }

        JPanel buttonPanel = new JPanel();
        applyBt = new JButton("Apply");
        applyBt.addActionListener(this);
        buttonPanel.add(applyBt);
        cancelBt = new JButton("Cancel");
        cancelBt.addActionListener(this);
        buttonPanel.add(cancelBt);

        Container contentPane = this.getContentPane();

        contentPane.setLayout(new BorderLayout());
        contentPane.add(optionsPanel, BorderLayout.CENTER);
        contentPane.add(buttonPanel, BorderLayout.SOUTH);
    }

    public void setProperties(CoGProperties props) {
        if (props == null) {
            throw new IllegalArgumentException("Properties cannot be null");
        }
        properties = props;
        optionsPanel.set(props);
    }

    public void actionPerformed(ActionEvent e) {
        Object source = e.getSource();
        if (source == cancelBt) {
            properties = null;
            dispose();
        } else if (source == applyBt && optionsPanel.validateSettings()) {
            optionsPanel.get(properties);
            dispose();
        }
    }

    public CoGProperties getProperties() {
        return properties;
    }

}
