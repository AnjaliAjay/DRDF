package jp.ac.titech.ylab.drdf.strategies;

import jp.ac.titech.ylab.drdf.DistributionStrategy;

public class HashDistributionStrategy implements DistributionStrategy {

    @Override
    public int distributeTo(String subjectOrObject, int numOfSlaves) {
        throw new UnsupportedOperationException("should be implemented");
    }

}
