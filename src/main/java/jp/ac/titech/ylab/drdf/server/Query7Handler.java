package jp.ac.titech.ylab.drdf.server;

import jp.ac.titech.ylab.drdf.LubmQuery7CopyManager;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.AbstractHandler;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

/**
 * Created by nishimura on 2015/04/23.
 */
public class Query7Handler extends AbstractHandler {

    private String mydb = "%sserver%d_%s";
    private int myid;

    private String[] destinations = new String[] {
            "jdbc:postgresql://192.168.172.174:5432/",
            "jdbc:postgresql://192.168.172.175:5432/",
            "jdbc:postgresql://192.168.172.176:5432/",
            "jdbc:postgresql://192.168.172.177:5432/"
    };

    public Query7Handler(int myid) {
        this.myid = myid;
    }

    @Override
    public void handle(String s, Request request, HttpServletRequest httpServletRequest, HttpServletResponse httpServletResponse) throws IOException, ServletException {

        if ("/drop".equals(s)) {
            dropTables(s, request, httpServletResponse);
        } else {
            runQuery(s, request, httpServletResponse);
        }
    }

    private void dropTables(String s, Request request, HttpServletResponse httpServletResponse) throws IOException {
        LubmQuery7CopyManager query7 = new LubmQuery7CopyManager(String.format(mydb, destinations[myid],myid+1, "lumb50"), myid, destinations);
        try {
            query7.dropTables(destinations);
            httpServletResponse.setStatus(HttpServletResponse.SC_ACCEPTED);
            httpServletResponse.setContentType("text/plain");
            httpServletResponse.getWriter().append("tables have been dropped.");
        } catch (Exception e) {
            httpServletResponse.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
            httpServletResponse.setContentType("text/plain");
            httpServletResponse.getWriter().append(e.getMessage());
        }
        request.setHandled(true);
    }

    private void runQuery(String s, Request request, HttpServletResponse httpServletResponse) throws IOException {
        String dataset = s.substring(1);
        LubmQuery7CopyManager query7 = new LubmQuery7CopyManager(String.format(mydb, destinations[myid],myid+1, dataset), myid, destinations);

        try {
            query7.run();
            httpServletResponse.setStatus(HttpServletResponse.SC_ACCEPTED);
            httpServletResponse.setContentType("text/plain");
        } catch (Exception e) {
            httpServletResponse.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
            httpServletResponse.setContentType("text/plain");
            httpServletResponse.getWriter().append(e.getMessage());
        }
        request.setHandled(true);
    }
}
