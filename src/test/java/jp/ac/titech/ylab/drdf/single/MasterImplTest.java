package jp.ac.titech.ylab.drdf.single;

import static org.junit.Assert.*;
import jp.ac.titech.ylab.drdf.DistributionStrategy;
import jp.ac.titech.ylab.drdf.Slave;
import jp.ac.titech.ylab.drdf.Triple;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class MasterImplTest {


  @Test
  public void testInsert() throws Exception {
    Slave slaveMock = Mockito.mock(Slave.class);
    MasterImpl testObj = new MasterImpl(slaveMock);
    Triple triple = new Triple("s", "p", "o");
    testObj.insert(triple);
    
    Mockito.verify(slaveMock).insertTripleDistributedBySubject(triple);
    Mockito.verify(slaveMock).insertTripleDistributedByObject(triple);
  }
  
  @Test
  public void testInsert2() throws Exception {
    
    Slave slaveMock1 = Mockito.mock(Slave.class);
    Slave slaveMock2 = Mockito.mock(Slave.class);
    DistributionStrategy strategyMock = Mockito.mock(DistributionStrategy.class);
    MasterImpl testObj = new MasterImpl(slaveMock1, slaveMock2);
    Triple triple1 = new Triple("s1", "p", "o1");
    Triple triple2 = new Triple("s2", "p", "o2");
    
    Mockito.when(strategyMock.distributeTo("s1", 2)).thenReturn(1);
    Mockito.when(strategyMock.distributeTo("s2", 2)).thenReturn(2);
    Mockito.when(strategyMock.distributeTo("o1", 2)).thenReturn(2);
    Mockito.when(strategyMock.distributeTo("o2", 2)).thenReturn(1);
    testObj.insert(triple1);
    testObj.insert(triple2);
    
    Mockito.verify(slaveMock1).insertTripleDistributedBySubject(triple1);
    Mockito.verify(slaveMock1).insertTripleDistributedByObject(triple2);
    Mockito.verify(slaveMock2).insertTripleDistributedBySubject(triple2);
    Mockito.verify(slaveMock2).insertTripleDistributedByObject(triple1);
  }
}
