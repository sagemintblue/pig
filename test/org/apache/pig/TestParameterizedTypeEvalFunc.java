package org.apache.pig;

import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.junit.Test;

import java.io.IOException;

public class TestParameterizedTypeEvalFunc {
    public abstract static class EvalFunc1<T> extends EvalFunc<T> {}
    public abstract static class EvalFunc2<X, T> extends EvalFunc1<T> {}
    public static class EvalFunc3<X> extends EvalFunc2<X, DataBag> {
        @Override
        public DataBag exec(Tuple input) throws IOException {
            return null;
        }
    }

    @Test
    public void testConstruction() {
        EvalFunc<DataBag> udf = new EvalFunc3();
    }
}
