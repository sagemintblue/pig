package org.apache.pig.tools.pigstats;

import com.twitter.twadoop.pig.stats.PigStatsDataVizCollector;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.handler.AbstractHandler;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;

/**
 * @author billg
 */
public class ScriptStatusServer implements Runnable {
    protected Log LOG = LogFactory.getLog(getClass());

    private int port;
    private PigStatsDataVizCollector statsCollector;
    private Server server;

    public ScriptStatusServer(PigStatsDataVizCollector statsCollector, int port) {
        this.port = port;
        this.statsCollector = statsCollector;
    }

    public void start() {
        try {
          Thread serverThread = new Thread(this);
          serverThread.setDaemon(false);
          serverThread.start();
        } catch (Exception e) {
          LOG.error("Could not start ScriptStatusServer", e);
        }
    }

    @Override
    public void run() {
        server = new Server(port);
        server.setHandler(new DAGHandler());
        server.setStopAtShutdown(true);

        try {
            server.start();
            server.join();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void stop() throws Exception {
        if (server != null) server.stop();
    }

    public class DAGHandler extends AbstractHandler {

        @Override
        public void handle(String target, HttpServletRequest request,
                           HttpServletResponse response, int distpatch) throws IOException, ServletException {
            response.setContentType("application/json");
            response.setStatus(HttpServletResponse.SC_OK);

            if (target.startsWith("/dag")) {
                Collection<PigStatsDataVizCollector.DAGNode> nodes = statsCollector.getDagNodeNameMap().values();
                sendJson(response, nodes.toArray(new PigStatsDataVizCollector.DAGNode[nodes.size()]));
            } else if (target.startsWith("/events")) {
                Integer sinceId = request.getParameter("sinceId") != null ?
                        Integer.getInteger(request.getParameter("sinceId")) : -1;
                Collection<PigStatsDataVizCollector.PigScriptEvent> events = statsCollector.getEventsSinceId(sinceId);
                sendJson(response, events.toArray(new PigStatsDataVizCollector.PigScriptEvent[events.size()]));
            } else {
                HashMap<String, String> errorMap = new HashMap<String,String>();
                errorMap.put("message", "Invalid request URI target: " + target);
                sendJson(response, errorMap);
            }
        }
    }

    private static void sendJson(HttpServletResponse response, Object object) throws IOException {
        ObjectMapper om = new ObjectMapper();
        om.configure(SerializationConfig.Feature.INDENT_OUTPUT, true);
        om.configure(SerializationConfig.Feature.WRAP_EXCEPTIONS, true);
        om.configure(SerializationConfig.Feature.FAIL_ON_EMPTY_BEANS, false);
        response.getWriter().println(om.writeValueAsString(object));
        response.getWriter().flush();
    }
}
