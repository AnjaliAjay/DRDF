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

    private int myid;

    public Query7Handler(int myid) {
        this.myid = myid;
    }

    @Override
    public void handle(String s, Request request, HttpServletRequest httpServletRequest, HttpServletResponse httpServletResponse) throws IOException, ServletException {

        if (s.startsWith("/drop/")) {
            dropTables(s, request, httpServletResponse);
        } else {
            runQuery(s, request, httpServletResponse);
        }
    }

    private void dropTables(String s, Request request, HttpServletResponse httpServletResponse) throws IOException {
        String dataset = s.substring(6);
        LubmQuery7CopyManager query7 = new LubmQuery7CopyManager(dataset, myid);
        try {
            query7.dropTables();
            httpServletResponse.setStatus(HttpServletResponse.SC_ACCEPTED);
            httpServletResponse.setContentType("text/plain");
            httpServletResponse.getWriter().append("tables have been dropped.");
        } catch (Exception e) {
            httpServletResponse.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
            httpServletResponse.setContentType("text/plain");
            httpServletResponse.getWriter().append(e.getMessage());
            e.printStackTrace();
        }
        request.setHandled(true);
    }

    private void runQuery(String s, Request request, HttpServletResponse httpServletResponse) throws IOException {
        String dataset = s.substring(1);
        LubmQuery7CopyManager query7 = new LubmQuery7CopyManager(dataset, myid);

        try {
            long startTime = System.nanoTime();
            String logBuf = query7.run();
            long endTime = System.nanoTime();

            httpServletResponse.setStatus(HttpServletResponse.SC_ACCEPTED);
            httpServletResponse.setContentType("text/plain");
            httpServletResponse.getWriter().append(String.format("execution time: %d ns\n", endTime - startTime)).append(logBuf);
        } catch (Exception e) {
            httpServletResponse.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
            httpServletResponse.setContentType("text/plain");
            httpServletResponse.getWriter().append(e.getMessage());
            e.printStackTrace();
        }
        request.setHandled(true);
    }
}
