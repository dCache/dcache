package org.dcache.chimera.namespace;

import com.sun.jersey.api.core.PackagesResourceConfig;
import com.sun.jersey.spi.container.servlet.ServletContainer;
import org.dcache.chimera.namespace.ws.ChimeraResource;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;

public class ChimeraWebApi {

    private int port;
    private ChimeraNameSpaceProvider provider;
    private Server server;

    public void init() throws Exception {
        server = new Server(port);
        ServletContextHandler contex = new ServletContextHandler(ServletContextHandler.NO_SESSIONS);
        contex.setContextPath("/");
        contex.addServlet(
                new ServletHolder(
                    new ServletContainer(
                        new PackagesResourceConfig(ChimeraResource.class.getPackage().getName())
                    )
                ), "/*");
        contex.setAttribute(ChimeraResource.FS, provider);
        server.setHandler(contex);
        server.start();
    }

    public void destoroy() throws Exception {
        server.stop();
    }

    public void setPort(int port) {
        this.port = port;
    }

    public void setProvider(ChimeraNameSpaceProvider provider) {
        this.provider = provider;
    }

}
