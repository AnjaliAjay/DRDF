package jp.ac.titech.ylab.drdf.server;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.ContextHandler;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import java.io.File;

/**
 * Created by nishimura on 2015/04/23.
 */
public class QueryExecutionServer {
    @Option(name="-port", usage="port number")
    private int port = 8080;

    @Option(name="-id", usage="slave id")
    private int id = 0;

    @Option(name="-server")
    private boolean isServer = false;

    public static void main(String[] args) throws Exception {
        QueryExecutionServer app = new QueryExecutionServer();

        CmdLineParser parser = new CmdLineParser(app);
        parser.parseArgument(args);

        if (app.isServer) {
            app.serverRun();
        } else {
            app.clientRun();
        }
    }

    public void serverRun() throws Exception {
        Server server = new Server(port);
        ContextHandler context = new ContextHandler("/query7");
        context.setHandler(new Query7Handler(id));
        server.setHandler(context);

        server.start();
        server.dumpStdErr();
        server.join();
    }

    public void clientRun() {

    }
}
