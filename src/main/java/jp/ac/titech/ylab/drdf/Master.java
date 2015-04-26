package jp.ac.titech.ylab.drdf;

public interface Master {
    void setDistributionStrategy(DistributionStrategy strategy);

    void insert(Triple triple);

    void query(String query);
}
