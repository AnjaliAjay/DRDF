package jp.ac.titech.ylab.drdf;

public interface Slave {
    void insertTripleDistributedBySubject(Triple triple);

    void insertTripleDistributedByObject(Triple triple);

    void execute(Command command);
}
