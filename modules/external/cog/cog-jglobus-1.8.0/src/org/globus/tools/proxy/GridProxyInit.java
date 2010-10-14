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

import java.io.File;
import java.io.OutputStream;
import java.io.FileOutputStream;
import java.awt.Frame;
import java.awt.GridLayout;
import java.awt.Container;
import java.awt.event.ActionListener;
import java.awt.event.ActionEvent;
import java.awt.event.WindowListener;
import java.awt.event.WindowAdapter;
import java.awt.event.WindowEvent;
import javax.swing.JOptionPane;
import javax.swing.JProgressBar;
import javax.swing.JDialog;
import javax.swing.JLabel;
import javax.swing.JButton;
import javax.swing.JPasswordField;
import javax.swing.JCheckBox;
import javax.swing.Timer;
import javax.swing.JPanel;
import javax.swing.BoxLayout;

import org.globus.gsi.GlobusCredential;
import org.globus.gsi.CertUtil;
import org.globus.util.Util;
import org.globus.tools.ui.util.UITools;

public class GridProxyInit extends JDialog implements ActionListener {

    private static final String PKCS11_MODEL = "org.globus.tools.proxy.PKCS11GridProxyModel";

    private final int MAX = 10;

    private JPasswordField passwordTF;

    private JButton optionsButton, cancelButton, createButton;

    private JCheckBox PKCS11Box = null;

    private GridProxyModel model = null;

    private ProxyListener proxyListener = null;

    private GlobusCredential proxy = null;

    private boolean runAsApplication = false;

    private boolean closeOnSuccess = false;

    private boolean saveProxy = true;

    public GridProxyInit(Frame owner, boolean modal) {
        super(owner, modal);
        init();
    }

    public GridProxyInit() {
        super();
        init();
    }

    private void init() {

        setTitle("Grid Proxy Init");

        optionsButton = new JButton("Options");
        optionsButton.addActionListener(this);

        passwordTF = new JPasswordField(15);
        passwordTF.addActionListener(this);

        JPanel passPanel = new JPanel();

        passPanel.add(new JLabel("Password: "));
        passPanel.add(passwordTF);
        passPanel.add(optionsButton);

        JPanel buttonPanel = new JPanel(new GridLayout(1, 2, 2, 2));
        createButton = new JButton("Create");
        cancelButton = new JButton("Cancel");

        JPanel btPanel = new JPanel();
        btPanel.add(buttonPanel);

        buttonPanel.add(createButton);
        buttonPanel.add(cancelButton);
        createButton.addActionListener(this);
        cancelButton.addActionListener(this);

        JPanel ibutPanel = new JPanel();
        PKCS11Box = new JCheckBox("Use PKCS11 Device");
        ibutPanel.add(PKCS11Box);

        Container contentPane = this.getContentPane();
        BoxLayout bl = new BoxLayout(contentPane, BoxLayout.Y_AXIS);
        contentPane.setLayout(bl);

        /*
         * contentPane.add(passPanel, BorderLayout.NORTH);
         * contentPane.add(ibutPanel, BorderLayout.CENTER);
         * contentPane.add(btPanel, BorderLayout.SOUTH);
         */

        contentPane.add(passPanel);
        contentPane.add(ibutPanel);
        contentPane.add(btPanel);

        checkPKCS11Support();
    }

    private void checkPKCS11Support() {
        try {
            Class iClass = Class.forName(PKCS11_MODEL);
            PKCS11Box.setEnabled(true);
        } catch (Exception e) {
            PKCS11Box.setEnabled(false);
        }
    }

    private void setCloseButtonText() {
        if (runAsApplication) {
            cancelButton.setText("Exit");
        } else {
            cancelButton.setText("Cancel");
        }
    }

    public void actionPerformed(ActionEvent evt) {
        Object source = evt.getSource();
        if (source == passwordTF) {
            source = createButton;
        }

        if (source == createButton) {

            if (!validateSettings())
                return;
            Thread th = (new Thread() {
                public void run() {
                    createButton.setEnabled(false);
                    cancelButton.setEnabled(false);
                    optionsButton.setEnabled(false);
                    createProxy(PKCS11Box.isSelected());
                    createButton.setEnabled(true);
                    cancelButton.setEnabled(true);
                    optionsButton.setEnabled(true);
                }
            });
            th.start();

        } else if (source == optionsButton) {
            model = getModel(PKCS11Box.isSelected());
            ProxyOptionsWindow wind = new ProxyOptionsWindow(this, true,
                    PKCS11Box.isSelected());
            wind.setProperties(model.getProperties());
            wind.pack();
            UITools.center(this, wind);
            wind.setVisible(true);

        } else if (source == cancelButton) {
            close(0);
        }
    }

    private boolean validateSettings() {
        char[] pwd = passwordTF.getPassword();
        if (pwd.length == 0) {
            JOptionPane.showMessageDialog(this, "Please enter your password.",
                    "Need More Information", JOptionPane.WARNING_MESSAGE);
            return false;
        }
        return true;
    }

    private GridProxyModel getModel(boolean usePKCS11Device) {
        if (usePKCS11Device) {
            try {
                Class iClass = Class.forName(PKCS11_MODEL);
                return (GridProxyModel) iClass.newInstance();
            } catch (Exception e) {
                return new DefaultGridProxyModel();
            }
        } else {
            return new DefaultGridProxyModel();
        }
    }

    class Task extends Thread {
        private boolean done = false;

        private Exception exception = null;

        private boolean cancel = false;

        private GlobusCredential proxy = null;

        public boolean isDone() {
            return done;
        }

        public void cancel() {
            cancel = true;
        }

        public boolean isCancelled() {
            return cancel;
        }

        public GlobusCredential getProxy() {
            return proxy;
        }

        public void run() {

            model = getModel(PKCS11Box.isSelected());

            try {
                proxy = model.createProxy(new String(passwordTF.getPassword()));
            } catch (Exception e) {
                exception = e;
                done = true;
                return;
            }

            if (cancel)
                return;

            if (saveProxy) {
                OutputStream out = null;
                String proxyFile = model.getProperties().getProxyFile();
                try {
                    File file = Util.createFile(proxyFile);
                    Util.setOwnerAccessOnly(proxyFile);
                    out = new FileOutputStream(file);
                    proxy.save(out);
                } catch (SecurityException e) {
                    exception = e;
                    done = true;
                    return;
                } catch (Exception e) {
                    exception = e;
                    done = true;
                    return;
                } finally {
                    if (out != null) {
                        try {
                            out.close();
                        } catch (Exception e) {
                        }
                    }
                }
                if (cancel)
                    return;
            }

            // send the event....
            if (proxyListener != null) {
                proxyListener.proxyCreated(proxy);
            }

            done = true;
        }

        public Exception getException() {
            return exception;
        }
    }

    private void createProxy(boolean usePKCS11Device) {

        final JLabel msgLabel = new JLabel("Creating proxy...");
        final JProgressBar progressBar = new JProgressBar(0, MAX);

        progressBar.setValue(0);

        Object[] comp = { msgLabel, progressBar };
        Object[] options = { "Cancel" };

        JOptionPane pane = new JOptionPane(comp, JOptionPane.DEFAULT_OPTION,
                JOptionPane.INFORMATION_MESSAGE, null, options, options[0]);

        final JDialog dialog = pane.createDialog(this, "Creating Proxy");

        final Task task = new Task();

        Timer timer = new Timer(250, new ActionListener() {
            public void actionPerformed(ActionEvent e) {
                if (task.isDone()) {
                    dialog.setVisible(false);
                } else {
                    progressBar.setValue((progressBar.getValue() + 1) % MAX);
                }
            }
        });

        timer.start();
        task.start();

        try {
            Thread.sleep(500);
        } catch (Exception e) {
        }

        Object selectedValue = null;

        if (task.getException() == null) {
            dialog.show();
            selectedValue = pane.getValue();
        } else {
            selectedValue = JOptionPane.UNINITIALIZED_VALUE;
        }

        timer.stop();

        if (selectedValue == null || selectedValue == options[0]) {
            // window closed by user or cancel pressed
            task.cancel(); // <-- this does not work!!!!
            proxy = null;
        } else if (selectedValue == JOptionPane.UNINITIALIZED_VALUE) {
            // task finished
            Exception e = task.getException();
            if (e == null) {
                proxy = task.getProxy();
                JOptionPane.showMessageDialog(this,
                        "Proxy was successfully created.", "Proxy Created",
                        JOptionPane.INFORMATION_MESSAGE);

                if (closeOnSuccess)
                    setVisible(false);
            } else {
                proxy = null;
                JOptionPane.showMessageDialog(this, "Failed to create proxy: "
                        + e.getMessage(), "Proxy Create Error",
                        JOptionPane.ERROR_MESSAGE);
            }
        } else {
            proxy = null;
        }

    }

    public void addProxyListener(ProxyListener listener) {
        proxyListener = listener;
    }

    public void setRunAsApplication(boolean runAsApp) {
        runAsApplication = runAsApp;
        setCloseButtonText();
    }

    public void setCloseOnSuccess(boolean close) {
        closeOnSuccess = close;
    }

    public void saveProxy(boolean saveProxy) {
        this.saveProxy = saveProxy;
    }

    public GlobusCredential getProxy() {
        return proxy;
    }

    private void close(int errorCode) {
        if (runAsApplication) {
            System.exit(errorCode);
        } else {
            setVisible(false);
        }
    }

    // ---------------------------------

    public static void main(String args[]) {
        for (int i = 0; i < args.length; i++) {
            if (args[i].equals("-help")) {
                System.out.println("Syntax: java GridProxyInit");
                System.exit(0);
            }
        }
        CertUtil.init();

        GridProxyInit gpiFrame = new GridProxyInit(null, true);
        gpiFrame.setRunAsApplication(true);
        gpiFrame.saveProxy(true);

        WindowListener l = new WindowAdapter() {
            public void windowClosing(WindowEvent e) {
                System.exit(0);
            }
        };

        gpiFrame.addWindowListener(l);
        gpiFrame.pack();
        UITools.center(null, gpiFrame);
        gpiFrame.setVisible(true);
    }

}
