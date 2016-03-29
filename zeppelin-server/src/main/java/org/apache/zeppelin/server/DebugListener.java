package org.apache.zeppelin.server;

import org.eclipse.jetty.util.component.AbstractLifeCycle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.*;
import javax.servlet.http.HttpServletRequest;
import java.util.Collections;

public class DebugListener extends AbstractLifeCycle implements ServletContextListener {

    private static String prepend = ">>>";
    private static final Logger LOG = LoggerFactory.getLogger(DebugListener.class);

    final ServletRequestListener servletRequestListener = new ServletRequestListener() {

        @Override
        public void requestDestroyed(ServletRequestEvent servletRequestEvent) {
        }

        @Override
        public void requestInitialized(ServletRequestEvent servletRequestEvent) {
            ServletContext c = servletRequestEvent.getServletContext();
            LOG.info(prepend + "attributes:::");
            for (String s: Collections.list(c.getAttributeNames())) {
                LOG.info(prepend + s + ": " + c.getAttribute(s));
            }
            HttpServletRequest r = (HttpServletRequest)servletRequestEvent.getServletRequest();
            log(prepend + "contextName: %s, servletContext: %s, pathInfo: %s, URI: %s",
                    c.getAttribute("name"),
                    c.getServletContextName(),
                    r.getPathInfo(),
                    r.getRequestURI());
        }
    };

    @Override
    public void contextInitialized(ServletContextEvent servletContextEvent) {
        servletContextEvent.getServletContext().addListener(servletRequestListener);
    }

    @Override
    public void contextDestroyed(ServletContextEvent servletContextEvent) {

    }

    private void log(String format, Object... arg) {
        LOG.info(String.format(format,arg));
    }
}
