/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable
 * law or agreed to in writing, software distributed under the License is distributed on an "AS IS"
 * BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License
 * for the specific language governing permissions and limitations under the License.
 */
package org.apache.pig.builtin;

import static org.apache.pig.builtin.mock.Storage.tuple;
import static org.apache.pig.builtin.mock.Storage.bag;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.builtin.TOP;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DefaultBagFactory;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
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

  @Test
  public void testBasic() throws IOException {
    TOP top = new TOP();
    TupleFactory tupleFactory = TupleFactory.getInstance();
    BagFactory bagFactory = DefaultBagFactory.getInstance();
    Tuple inputTuple = tupleFactory.newTuple(3);
    DataBag dBag = bagFactory.newDefaultBag();

    // set N = 10 i.e retain top 10 tuples
    inputTuple.set(0, 10);
    // compare tuples by field number 1
    inputTuple.set(1, 1);
    // set the data bag containing the tuples
    inputTuple.set(2, dBag);

    // generate tuples of the form (group-1, 1), (group-2, 2) ...
    for (long i = 0; i < 100; i++) {
      Tuple nestedTuple = tupleFactory.newTuple(2);
      nestedTuple.set(0, "group-" + i);
      nestedTuple.set(1, i);
      dBag.add(nestedTuple);
    }

    DataBag outBag = top.exec(inputTuple);
    assertEquals(outBag.size(), 10L);
    checkItemsGT(outBag, 1, 89);

    // two initial results
    Tuple init1 = (new TOP.Initial()).exec(inputTuple);
    Tuple init2 = (new TOP.Initial()).exec(inputTuple);
    // two intermediate results

    DataBag intermedBag = bagFactory.newDefaultBag();
    intermedBag.add(init1);
    intermedBag.add(init2);
    Tuple intermedInput = tupleFactory.newTuple(intermedBag);
    Tuple intermedOutput1 = (new TOP.Intermed()).exec(intermedInput);
    Tuple intermedOutput2 = (new TOP.Intermed()).exec(intermedInput);
    checkItemsGT((DataBag) intermedOutput1.get(2), 1, 94);

    // final result
    DataBag finalInputBag = bagFactory.newDefaultBag();
    finalInputBag.add(intermedOutput1);
    finalInputBag.add(intermedOutput2);
    Tuple finalInput = tupleFactory.newTuple(finalInputBag);
    outBag = (new TOP.Final()).exec(finalInput);
    assertEquals(outBag.size(), 10L);
    checkItemsGT(outBag, 1, 96);
  }

  private void checkItemsGT(Iterable<Tuple> tuples, int field, int limit) throws ExecException {
    for (Tuple t : tuples) {
      Long val = (Long) t.get(field);
      assertTrue("Value " + val + " exceeded the expected limit", val > limit);
    }
  }
}
