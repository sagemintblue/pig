package org.apache.pig.test;

import static org.apache.pig.builtin.mock.Storage.tuple;
import static org.apache.pig.builtin.mock.Storage.bag;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertEquals;

import java.io.IOException;

import org.apache.pig.builtin.TOP;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.junit.Test;

/**
 * Unit tests for {@link TOP}.
 */
public class TestTOP {
  @Test
  public void testExecWithNull() throws IOException {
    assertNull(new TOP().exec(tuple(1, 0, null)));
  }

  @Test
  public void testInitialExecWithNull() throws IOException {
    assertNull(new TOP.Initial().exec(tuple(1, 0, null)));
  }

  @Test
  public void testIntermedExecWithNull() throws IOException {
    Tuple out = new TOP.Intermed().exec(tuple(bag(tuple(1, 0, null))));
    assertNotNull(out);
    assertEquals(3, out.size());
    DataBag bag = (DataBag) out.get(2);
    assertNull(bag);
  }

  @Test
  public void testFinalExecWithNull() throws IOException {
    DataBag bag = new TOP.Final().exec(tuple(bag(tuple(1, 0, null))));
    assertNull(bag);
  }

  @Test
  public void testFinalExecWithNullAndNonNull() throws IOException {
    DataBag bag = new TOP.Final().exec(tuple(bag(
        tuple(1, 0, bag(tuple(2))),
        tuple(1, 0, bag(tuple(3))),
        tuple(1, 0, null)
        )));
    assertNotNull(bag);
    assertEquals(1, bag.size());
    assertEquals(3, bag.iterator().next().get(0));
  }
}
