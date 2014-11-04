package jp.ac.titech.ylab.drdf;

public interface DistributionStrategy {
  int distributeTo(String subjectOrObject, int numOfSlaves);
}
