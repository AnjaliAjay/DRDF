package jp.ac.titech.ylab.drdf.single;

import java.util.ArrayList;
import java.util.List;

import jp.ac.titech.ylab.drdf.DistributionStrategy;
import jp.ac.titech.ylab.drdf.Master;
import jp.ac.titech.ylab.drdf.Slave;
import jp.ac.titech.ylab.drdf.Triple;

public class MasterImpl implements Master {

  private final List<Slave> slaves = new ArrayList<>();

  public MasterImpl(Slave... slaves) {
    for (Slave slave : slaves) {
      this.slaves.add(slave);
    }
  }

  @Override
  public void insert(Triple triple) {
    slaves.get(0).insertTripleDistributedBySubject(triple);
  }

  @Override
  public void query(String query) {
    throw new UnsupportedOperationException("should be implemented");
  }

  @Override
  public void setDistributionStrategy(DistributionStrategy strategy) {
    throw new UnsupportedOperationException("should be implemented");
  }

}
