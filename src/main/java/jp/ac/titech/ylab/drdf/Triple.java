package jp.ac.titech.ylab.drdf;

public class Triple {
    public final int id;
    public final String subject;
    public final String predicate;
    public final String object;

    public Triple(int id, String subject, String predicate, String object) {
        super();
        this.id = id;
        this.subject = subject;
        this.predicate = predicate;
        this.object = object;
    }

    public String format(String format) {
        return String.format(format, id, subject, predicate, object);
    }
}
