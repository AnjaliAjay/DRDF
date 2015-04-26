package jp.ac.titech.ylab.drdf;

import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertThat;
import static org.mockito.MockitoAnnotations.initMocks;

public class AppTest {

    final App app = new App();

    @Before
    public void init() {
        initMocks(this);
    }

    @Test
    public void can() {
        assertThat(app, notNullValue());
    }
}

