/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.pig.test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;

import junit.framework.Assert;

import org.apache.pig.EvalFunc;
import org.apache.pig.ExecType;
import org.apache.pig.PigException;
import org.apache.pig.PigServer;
import org.apache.pig.builtin.BinStorage;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.DefaultBagFactory;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.io.FileLocalizer;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.util.LogUtils;
import org.apache.pig.test.utils.Identity;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class TestEvalPipeline2 {
    
    static MiniCluster cluster = MiniCluster.buildCluster();
    private PigServer pigServer;

    TupleFactory mTf = TupleFactory.getInstance();
    BagFactory mBf = BagFactory.getInstance();
    
    @Before
    public void setUp() throws Exception{
        FileLocalizer.setR(new Random());
        pigServer = new PigServer(ExecType.MAPREDUCE, cluster.getProperties());
//        pigServer = new PigServer(ExecType.LOCAL);
    }
    
    @AfterClass
    public static void oneTimeTearDown() throws Exception {
        cluster.shutDown();
    }
    
    @Test
    public void testUdfInputOrder() throws IOException {
        String[] input = {
                "(123)",
                "((123)",
                "(123123123123)",
                "(asdf)"
        };
        
        Util.createInputFile(cluster, "table_udfInp", input);
        pigServer.registerQuery("a = load 'table_udfInp' as (i:int);");
        pigServer.registerQuery("b = foreach a {dec = 'hello'; str1 = " +  Identity.class.getName() + 
                    "(dec,'abc','def');" + 
                    "generate dec,str1; };");
        Iterator<Tuple> it = pigServer.openIterator("b");
        
        Tuple tup=null;

        //tuple 1 
        tup = it.next();
        Tuple out = (Tuple)tup.get(1);

        Assert.assertEquals( out.get(0).toString(), "hello");
        Assert.assertEquals(out.get(1).toString(), "abc");
        Assert.assertEquals(out.get(2).toString(), "def");
        
        Util.deleteFile(cluster, "table_udfInp");
    }
 

    @Test
    public void testUDFwithStarInput() throws Exception {
        int LOOP_COUNT = 10;
        File tmpFile = Util.createTempFileDelOnExit("test", "txt");
        PrintStream ps = new PrintStream(new FileOutputStream(tmpFile));
        for(int i = 0; i < LOOP_COUNT; i++) {
            for(int j=0;j<LOOP_COUNT;j+=2){
                ps.println(i+"\t"+j);
                ps.println(i+"\t"+j);
            }
        }
        ps.close();

        pigServer.registerQuery("A = LOAD '"
                + Util.generateURI(tmpFile.toString(), pigServer
                        .getPigContext()) + "';");
        pigServer.registerQuery("B = group A by $0;");
        String query = "C = foreach B {"
        + "generate " + Identity.class.getName() +"(*);"
        + "};";

        pigServer.registerQuery(query);
        Iterator<Tuple> iter = pigServer.openIterator("C");
        if(!iter.hasNext()) Assert.fail("No output found");
        int numIdentity = 0;
        while(iter.hasNext()){
            Tuple tuple = iter.next();
            Tuple t = (Tuple)tuple.get(0);
            Assert.assertEquals(DataByteArray.class, t.get(0).getClass());
            int group = Integer.parseInt(new String(((DataByteArray)t.get(0)).get()));
            Assert.assertEquals(numIdentity, group);
            Assert.assertTrue(t.get(1) instanceof DataBag);
            DataBag bag = (DataBag)t.get(1);
            Assert.assertEquals(10, bag.size());
            Assert.assertEquals(2, t.size());
            ++numIdentity;
        }
        Assert.assertEquals(LOOP_COUNT, numIdentity);

    }
    @Test
    public void testBinStorageByteArrayCastsSimple() throws IOException {
        // Test for PIG-544 fix
        // Tries to read data in BinStorage bytearrays as other pig types,
        // should return null if the conversion fails.
        // This test case does not use a practical example , it just tests
        // if the conversion happens when minimum conditions for conversion
        // such as expected number of bytes are met.
        String[] input = {
                    "asdf\t12\t1.1\t231\t234", 
                    "sa\t1231\t123.4\t12345678\t1234.567",
                    "asdff\t1232123\t1.45345\t123456789\t123456789.9"
                    };
        
        Util.createInputFile(cluster, "table_bs_ac", input);

        // test with BinStorage
        pigServer.registerQuery("a = load 'table_bs_ac';");
        String output = "/pig/out/TestEvalPipeline2_BinStorageByteArrayCasts";
        pigServer.deleteFile(output);
        pigServer.store("a", output, BinStorage.class.getName());

        pigServer.registerQuery("b = load '" + output + "' using BinStorage('Utf8StorageConverter') "
                + "as (name: int, age: int, gpa: float, lage: long, dgpa: double);");
        
        Iterator<Tuple> it = pigServer.openIterator("b");
        
        Tuple tup=null;
        
        // I have separately verified only few of the successful conversions,
        // assuming the rest are correct.
        // It is primarily testing if null is being returned when conversions
        // are expected to fail
        
        //tuple 1 
        tup = it.next();

        Assert.assertTrue((Integer)tup.get(0) == null); 
        Assert.assertTrue((Integer)tup.get(1) == 12);
        Assert.assertTrue((Float)tup.get(2) == 1.1F);
        Assert.assertTrue((Long)tup.get(3) == 231L);
        Assert.assertTrue((Double)tup.get(4) == 234.0);
        
        //tuple 2 
        tup = it.next();
        Assert.assertTrue(tup.get(0) == null);
        Assert.assertTrue((Integer)tup.get(1) == 1231);
        Assert.assertTrue((Float)tup.get(2) == 123.4F);
        Assert.assertTrue((Long)tup.get(3) == 12345678L);
        Assert.assertTrue((Double)tup.get(4) == 1234.567);
        
        //tuple 3
        tup = it.next();
        Assert.assertTrue(tup.get(0) == null);
        Assert.assertTrue((Integer)tup.get(1) == 1232123);
        Assert.assertTrue((Float)tup.get(2) == 1.45345F);
        Assert.assertTrue((Long)tup.get(3) == 123456789L);
        Assert.assertTrue((Double)tup.get(4) == 1.234567899E8);
        
        Util.deleteFile(cluster, "table");
    }
    @Test
    public void testBinStorageByteArrayCastsComplexBag() throws IOException {
        // Test for PIG-544 fix
        
        // Tries to read data in BinStorage bytearrays as other pig bags,
        // should return null if the conversion fails.
        
        String[] input = {
                "{(asdf)}",
                "{(2344)}",
                "{(2344}",
                "{(323423423423434)}",
                "{(323423423423434L)}",
                "{(asdff)}"
        };
        
        Util.createInputFile(cluster, "table_bs_ac_clx", input);

        // test with BinStorage
        pigServer.registerQuery("a = load 'table_bs_ac_clx' as (f1);");
        pigServer.registerQuery("b = foreach a generate (bag{tuple(int)})f1;");
        
        Iterator<Tuple> it = pigServer.openIterator("b");
        
        Tuple tup=null;

        //tuple 1 
        tup = it.next();
        Assert.assertTrue(tup.get(0) != null);
        
        //tuple 2 
        tup = it.next();
        Assert.assertTrue(tup.get(0) != null);
        
        //tuple 3 - malformed
        tup = it.next();
        Assert.assertTrue(tup.get(0) == null);

        //tuple 4 - integer exceeds size limit
        tup = it.next();
        Assert.assertTrue(tup.get(0) instanceof DataBag);
        DataBag db = (DataBag)tup.get(0);
        Assert.assertTrue(db.iterator().hasNext());
        Tuple innerTuple = (Tuple)db.iterator().next();
        Assert.assertTrue(innerTuple.get(0)==null);

        //tuple 5 
        tup = it.next();
        Assert.assertTrue(tup.get(0) != null);

        //tuple 6
        tup = it.next();
        Assert.assertTrue(tup.get(0) != null);
        
        Util.deleteFile(cluster, "table_bs_ac_clx");
    }
    @Test
    public void testBinStorageByteArrayCastsComplexTuple() throws IOException {
        // Test for PIG-544 fix
        
        // Tries to read data in BinStorage bytearrays as other pig bags,
        // should return null if the conversion fails.
        
        String[] input = {
                "(123)",
                "((123)",
                "(123123123123)",
                "(asdf)"
        };
        
        Util.createInputFile(cluster, "table_bs_ac_clxt", input);

        // test with BinStorage
        pigServer.registerQuery("a = load 'table_bs_ac_clxt' as (t:tuple(t:tuple(i:int)));");
        Iterator<Tuple> it = pigServer.openIterator("a");
        
        Tuple tup=null;

        //tuple 1 
        tup = it.next();
        Assert.assertTrue(tup.get(0) == null);
        
        //tuple 2 -malformed tuple
        tup = it.next();
        Assert.assertTrue(tup.get(0) == null);
        
        //tuple 3 - integer exceeds size limit
        tup = it.next();
        Assert.assertTrue(tup.get(0) == null);

        //tuple 4
        tup = it.next();
        Assert.assertTrue(tup.get(0) == null);

        Util.deleteFile(cluster, "table_bs_ac_clxt");
    }
    
    @Test
    public void testPigStorageWithCtrlChars() throws Exception {
        String[] inputData = { "hello\u0001world", "good\u0001morning", "nice\u0001day" };
        Util.createInputFile(cluster, "testPigStorageWithCtrlCharsInput.txt", inputData);
        String script = "a = load 'testPigStorageWithCtrlCharsInput.txt' using PigStorage('\u0001');" +
        		"b = foreach a generate $0, CONCAT($0, '\u0005'), $1; " +
        		"store b into 'testPigStorageWithCtrlCharsOutput.txt' using PigStorage('\u0001');" +
        		"c = load 'testPigStorageWithCtrlCharsOutput.txt' using PigStorage('\u0001') as (f1:chararray, f2:chararray, f3:chararray);";
        Util.registerMultiLineQuery(pigServer, script);
        Iterator<Tuple> it  = pigServer.openIterator("c");
        HashMap<String, Tuple> expectedResults = new HashMap<String, Tuple>();
        expectedResults.put("hello", (Tuple) Util.getPigConstant("('hello','hello\u0005','world')"));
        expectedResults.put("good", (Tuple) Util.getPigConstant("('good','good\u0005','morning')"));
        expectedResults.put("nice", (Tuple) Util.getPigConstant("('nice','nice\u0005','day')"));
        HashMap<String, Boolean> seen = new HashMap<String, Boolean>();
        int numRows = 0;
        while(it.hasNext()) {
            Tuple t = it.next();
            String firstCol = (String) t.get(0);
            Assert.assertFalse(seen.containsKey(firstCol));
            seen.put(firstCol, true);
            Assert.assertEquals(expectedResults.get(firstCol), t);
            numRows++;
        }
        Assert.assertEquals(3, numRows);
        Util.deleteFile(cluster, "testPigStorageWithCtrlCharsInput.txt");
    }

    @Test
    // Test case added for PIG-850
    public void testLimitedSortWithDump() throws Exception{
        int LOOP_COUNT = 40;
        File tmpFile = Util.createTempFileDelOnExit("test", "txt");
        PrintStream ps = new PrintStream(new FileOutputStream(tmpFile));
        Random r = new Random(1);
        int rand;
        for(int i = 0; i < LOOP_COUNT; i++) {
            rand = r.nextInt(100);
            ps.println(rand);
        }
        ps.close();

        pigServer.registerQuery("A = LOAD '"
                + Util.generateURI(tmpFile.toString(), pigServer
                        .getPigContext()) + "' AS (num:int);");
        pigServer.registerQuery("B = order A by num parallel 2;");
        pigServer.registerQuery("C = limit B 10;");
        Iterator<Tuple> result = pigServer.openIterator("C");
        
        int numIdentity = 0;
        while (result.hasNext())
        {
            result.next();
            ++numIdentity;
        }
        Assert.assertEquals(10, numIdentity);
    }

    @Test
    public void testLimitAfterSort() throws Exception{
        int LOOP_COUNT = 40;
        File tmpFile = Util.createTempFileDelOnExit("test", "txt");
        PrintStream ps = new PrintStream(new FileOutputStream(tmpFile));
        Random r = new Random(1);
        int rand;
        for(int i = 0; i < LOOP_COUNT; i++) {
            rand = r.nextInt(100);
            ps.println(rand);
        }
        ps.close();

        pigServer.registerQuery("A = LOAD '"
                + Util.generateURI(tmpFile.toString(), pigServer
                        .getPigContext()) + "' AS (num:int);");
        pigServer.registerQuery("B = order A by num parallel 2;");
        pigServer.registerQuery("C = limit B 10;");
        Iterator<Tuple> iter = pigServer.openIterator("C");
        if(!iter.hasNext()) Assert.fail("No output found");
        int numIdentity = 0;
        int oldNum = Integer.MIN_VALUE;
        int newNum;
        while(iter.hasNext()){
            Tuple t = iter.next();
            newNum = (Integer)t.get(0);
            Assert.assertTrue(newNum>=oldNum);
            oldNum = newNum;
            ++numIdentity;
        }
        Assert.assertEquals(10, numIdentity);
    }

    @Test
    public void testLimitAfterSortDesc() throws Exception{
        int LOOP_COUNT = 40;
        File tmpFile = Util.createTempFileDelOnExit("test", "txt");
        PrintStream ps = new PrintStream(new FileOutputStream(tmpFile));
        Random r = new Random(1);
        int rand;
        for(int i = 0; i < LOOP_COUNT; i++) {
            rand = r.nextInt(100);
            ps.println(rand);
        }
        ps.close();

        pigServer.registerQuery("A = LOAD '"
                + Util.generateURI(tmpFile.toString(), pigServer
                        .getPigContext()) + "' AS (num:int);");
        pigServer.registerQuery("B = order A by num desc parallel 2;");
        pigServer.registerQuery("C = limit B 10;");
        Iterator<Tuple> iter = pigServer.openIterator("C");
        if(!iter.hasNext()) Assert.fail("No output found");
        int numIdentity = 0;
        int oldNum = Integer.MAX_VALUE;
        int newNum;
        while(iter.hasNext()){
            Tuple t = iter.next();
            newNum = (Integer)t.get(0);
            Assert.assertTrue(newNum<=oldNum);
            oldNum = newNum;
            ++numIdentity;
        }
        Assert.assertEquals(10, numIdentity);
    }

    @Test
    // See PIG-894
    public void testEmptySort() throws Exception{
        File tmpFile = Util.createTempFileDelOnExit("test", "txt");
        pigServer.registerQuery("A = LOAD '"
                + Util.generateURI(tmpFile.toString(), pigServer
                        .getPigContext()) + "';");
        pigServer.registerQuery("B = order A by $0;");
        Iterator<Tuple> iter = pigServer.openIterator("B");
        
        Assert.assertTrue(iter.hasNext()==false);
    }
    
    // See PIG-761
    @Test
    public void testLimitPOPackageAnnotator() throws Exception{
        File tmpFile1 = Util.createTempFileDelOnExit("test1", "txt");
        PrintStream ps1 = new PrintStream(new FileOutputStream(tmpFile1));
        ps1.println("1\t2\t3");
        ps1.println("2\t5\t2");
        ps1.close();
        
        File tmpFile2 = Util.createTempFileDelOnExit("test2", "txt");
        PrintStream ps2 = new PrintStream(new FileOutputStream(tmpFile2));
        ps2.println("1\t1");
        ps2.println("2\t2");
        ps2.close();

        pigServer.registerQuery("A = LOAD '" + Util.generateURI(tmpFile1.toString(), pigServer.getPigContext()) + "' AS (a0, a1, a2);");
        pigServer.registerQuery("B = LOAD '" + Util.generateURI(tmpFile2.toString(), pigServer.getPigContext()) + "' AS (b0, b1);");
        pigServer.registerQuery("C = LIMIT B 100;");
        pigServer.registerQuery("D = COGROUP C BY b0, A BY a0 PARALLEL 2;");
        Iterator<Tuple> iter = pigServer.openIterator("D");
        
        Assert.assertTrue(iter.hasNext());
        Tuple t = iter.next();
        
        Assert.assertTrue(t.toString().equals("(2,{(2,2)},{(2,5,2)})"));
        
        Assert.assertTrue(iter.hasNext());
        t = iter.next();
        
        Assert.assertTrue(t.toString().equals("(1,{(1,1)},{(1,2,3)})"));
        
        Assert.assertFalse(iter.hasNext());
    }

    // See PIG-1195
    @Test
    public void testNestedDescSort() throws Exception{
        Util.createInputFile(cluster, "table_testNestedDescSort", new String[]{"3","4"});
        pigServer.registerQuery("A = LOAD 'table_testNestedDescSort' as (a0:int);");
        pigServer.registerQuery("B = group A ALL;");
        pigServer.registerQuery("C = foreach B { D = order A by a0 desc;generate D;};");
        Iterator<Tuple> iter = pigServer.openIterator("C");
        
        Assert.assertTrue(iter.hasNext());
        Tuple t = iter.next();
        
        Assert.assertTrue(t.toString().equals("({(4),(3)})"));
        Assert.assertFalse(iter.hasNext());
        
        Util.deleteFile(cluster, "table_testNestedDescSort");
    }
    
    // See PIG-282
    @Test
    public void testCustomPartitionerParseJoins() throws Exception{
    	String[] input = {
                "1\t3",
                "1\t2"
        };
        Util.createInputFile(cluster, "table_testCustomPartitionerParseJoins", input);
        
        // Custom Partitioner is not allowed for skewed joins, will throw a ExecException 
        try {
            pigServer.registerQuery("A = LOAD 'table_testCustomPartitionerParseJoins' as (a0:int, a1:int);");
            pigServer.registerQuery("B = ORDER A by $0;");
        	pigServer.registerQuery("skewed = JOIN A by $0, B by $0 USING 'skewed' PARTITION BY org.apache.pig.test.utils.SimpleCustomPartitioner;");
        	//control should not reach here
        	Assert.fail("Skewed join cannot accept a custom partitioner");
        } catch(FrontendException e) {
        	Assert.assertTrue( e.getMessage().contains( "Custom Partitioner is not supported for skewed join" ) );
		}
        
        pigServer.registerQuery("hash = JOIN A by $0, B by $0 USING 'hash' PARTITION BY org.apache.pig.test.utils.SimpleCustomPartitioner;");
        Iterator<Tuple> iter = pigServer.openIterator("hash");
        Tuple t;
        
        Collection<String> results = new HashSet<String>();
        results.add("(1,3,1,2)");
        results.add("(1,3,1,3)");
        results.add("(1,2,1,2)");
        results.add("(1,2,1,3)");
        
        Assert.assertTrue(iter.hasNext());
        t = iter.next();
        Assert.assertTrue(t.size()==4);
        Assert.assertTrue(results.contains(t.toString()));
        
        Assert.assertTrue(iter.hasNext());
        t = iter.next();
        Assert.assertTrue(t.size()==4);
        Assert.assertTrue(results.contains(t.toString()));
        
        Assert.assertTrue(iter.hasNext());
        t = iter.next();
        Assert.assertTrue(t.size()==4);
        Assert.assertTrue(results.contains(t.toString()));
        
        Assert.assertTrue(iter.hasNext());
        t = iter.next();
        Assert.assertTrue(t.size()==4);
        Assert.assertTrue(results.contains(t.toString()));
        
        // No checks are made for merged and replicated joins as they are compiled to a map only job 
        // No frontend error checking has been added for these jobs, hence not adding any test cases 
        // Manually tested the sanity once. Above test should cover the basic sanity of the scenario 
        
        Util.deleteFile(cluster, "table_testCustomPartitionerParseJoins");
    }
    
    // See PIG-282
    @Test
    public void testCustomPartitionerGroups() throws Exception{
    	String[] input = {
                "1\t1",
                "2\t1",
                "3\t1",
                "4\t1"
        };
        Util.createInputFile(cluster, "table_testCustomPartitionerGroups", input);
        
        pigServer.registerQuery("A = LOAD 'table_testCustomPartitionerGroups' as (a0:int, a1:int);");
        
        // It should be noted that for a map reduce job, the total number of partitions 
        // is the same as the number of reduce tasks for the job. Hence we need to find a case wherein 
        // we will get more than one reduce job so that we can use the partitioner. 	
        // The following logic assumes that we get 2 reduce jobs, so that we can hard-code the logic.
        //
        pigServer.registerQuery("B = group A by $0 PARTITION BY org.apache.pig.test.utils.SimpleCustomPartitioner parallel 2;");
        
        pigServer.store("B", "tmp_testCustomPartitionerGroups");
        
        new File("tmp_testCustomPartitionerGroups").mkdir();
        
        // SimpleCustomPartitioner partitions as per the parity of the key
        // Need to change this in SimpleCustomPartitioner is changed
        Util.copyFromClusterToLocal(cluster, "tmp_testCustomPartitionerGroups/part-r-00000", "tmp_testCustomPartitionerGroups/part-r-00000");
        BufferedReader reader = new BufferedReader(new FileReader("tmp_testCustomPartitionerGroups/part-r-00000"));
 	    String line = null;      	     
 	    while((line = reader.readLine()) != null) {
 	        String[] cols = line.split("\t");
 	        int value = Integer.parseInt(cols[0]) % 2;
 	        Assert.assertEquals(0, value);
 	    }
 	    Util.copyFromClusterToLocal(cluster, "tmp_testCustomPartitionerGroups/part-r-00001", "tmp_testCustomPartitionerGroups/part-r-00001");
        reader = new BufferedReader(new FileReader("tmp_testCustomPartitionerGroups/part-r-00001"));
 	    line = null;      	     
 	    while((line = reader.readLine()) != null) {
 	        String[] cols = line.split("\t");
 	        int value = Integer.parseInt(cols[0]) % 2;
 	        Assert.assertEquals(1, value);
 	    } 
        Util.deleteDirectory(new File("tmp_testCustomPartitionerGroups"));
        Util.deleteFile(cluster, "table_testCustomPartitionerGroups");
    }
    
    // See PIG-282
    @Test
    public void testCustomPartitionerCross() throws Exception{
    	String[] input = {
                "1\t3",
                "1\t2",
        };
    	
        Util.createInputFile(cluster, "table_testCustomPartitionerCross", input);
        pigServer.registerQuery("A = LOAD 'table_testCustomPartitionerCross' as (a0:int, a1:int);");
        pigServer.registerQuery("B = ORDER A by $0;");
        pigServer.registerQuery("C = cross A , B PARTITION BY org.apache.pig.test.utils.SimpleCustomPartitioner;");
        Iterator<Tuple> iter = pigServer.openIterator("C");
        Tuple t;
        
        Collection<String> results = new HashSet<String>();
        results.add("(1,3,1,2)");
        results.add("(1,3,1,3)");
        results.add("(1,2,1,2)");
        results.add("(1,2,1,3)");
        
        Assert.assertTrue(iter.hasNext());
        t = iter.next();
        Assert.assertTrue(t.size()==4);
        Assert.assertTrue(results.contains(t.toString()));
        
        Assert.assertTrue(iter.hasNext());
        t = iter.next();
        Assert.assertTrue(t.size()==4);
        Assert.assertTrue(results.contains(t.toString()));
        
        Assert.assertTrue(iter.hasNext());
        t = iter.next();
        Assert.assertTrue(t.size()==4);
        Assert.assertTrue(results.contains(t.toString()));
        
        Assert.assertTrue(iter.hasNext());
        t = iter.next();
        Assert.assertTrue(t.size()==4);
        Assert.assertTrue(results.contains(t.toString()));
        
        Util.deleteFile(cluster, "table_testCustomPartitionerCross");
    }
    
    // See PIG-972
    @Test
    public void testDescribeNestedAlias() throws Exception{
        String[] input = {
                "1\t3",
                "2\t4",
                "3\t5"
        };
        
        Util.createInputFile(cluster, "table_testDescribeNestedAlias", input);
        pigServer.registerQuery("A = LOAD 'table_testDescribeNestedAlias' as (a0, a1);");
        pigServer.registerQuery("P = GROUP A by a1;");
        // Test RelationalOperator
        pigServer.registerQuery("B = FOREACH P { D = ORDER A by $0; generate group, D.$0; };");
        
        // Test ExpressionOperator - negative test case
        pigServer.registerQuery("C = FOREACH A { D = a0/a1; E=a1/a0; generate E as newcol; };");
        Schema schema = pigServer.dumpSchemaNested("B", "D");
        Assert.assertTrue(schema.toString().equalsIgnoreCase("{a0: bytearray,a1: bytearray}"));
        try {
            schema = pigServer.dumpSchemaNested("C", "E");
        } catch (FrontendException e) {
            Assert.assertTrue(e.getErrorCode() == 1113);
        }
    }
    
    // See PIG-1484
    @Test
    public void testBinStorageCommaSeperatedPath() throws Exception{
        String[] input = {
                "1\t3",
                "2\t4",
                "3\t5"
        };
        
        Util.createInputFile(cluster, "table_simple1", input);
        pigServer.setBatchOn();
        pigServer.registerQuery("A = LOAD 'table_simple1' as (a0, a1);");
        pigServer.registerQuery("store A into 'table_simple1.bin' using BinStorage();");
        pigServer.registerQuery("store A into 'table_simple2.bin' using BinStorage();");

        pigServer.executeBatch();
        
        pigServer.registerQuery("A = LOAD 'table_simple1.bin,table_simple2.bin' using BinStorage();");
        Iterator<Tuple> iter = pigServer.openIterator("A");
        
        Tuple t = iter.next();
        Assert.assertTrue(t.toString().equals("(1,3)"));
        
        t = iter.next();
        Assert.assertTrue(t.toString().equals("(2,4)"));
        
        t = iter.next();
        Assert.assertTrue(t.toString().equals("(3,5)"));
        
        t = iter.next();
        Assert.assertTrue(t.toString().equals("(1,3)"));
        
        t = iter.next();
        Assert.assertTrue(t.toString().equals("(2,4)"));
        
        t = iter.next();
        Assert.assertTrue(t.toString().equals("(3,5)"));
        
        Assert.assertFalse(iter.hasNext());
    }
    
    // See PIG-1543
    @Test
    public void testEmptyBagIterator() throws Exception{
        String[] input1 = {
                "1",
                "1",
                "1"
        };
        
        String[] input2 = {
                "2",
                "2"
        };
        
        Util.createInputFile(cluster, "input1", input1);
        Util.createInputFile(cluster, "input2", input2);
        pigServer.registerQuery("A = load 'input1' as (a1:int);");
        pigServer.registerQuery("B = load 'input2' as (b1:int);");
        pigServer.registerQuery("C = COGROUP A by a1, B by b1;");
        pigServer.registerQuery("C1 = foreach C { Alim = limit A 1; Blim = limit B 1; generate Alim, Blim; };");
        pigServer.registerQuery("D1 = FOREACH C1 generate Alim,Blim, (IsEmpty(Alim)? 0:1), (IsEmpty(Blim)? 0:1), COUNT(Alim), COUNT(Blim);");
        
        Iterator<Tuple> iter = pigServer.openIterator("D1");
        
        Tuple t = iter.next();
        Assert.assertTrue(t.toString().equals("({(1)},{},1,0,1,0)"));
        
        t = iter.next();
        Assert.assertTrue(t.toString().equals("({},{(2)},0,1,0,1)"));
        
        Assert.assertFalse(iter.hasNext());
    }
    
    // See PIG-1669
    @Test
    public void testPushUpFilterScalar() throws Exception{
        String[] input1 = {
                "jason\t14\t4.7",
                "jack\t18\t4.6"
        };
        
        String[] input2 = {
                "jason\t14",
                "jack\t18"
        };
        
        Util.createInputFile(cluster, "table_PushUpFilterScalar1", input1);
        Util.createInputFile(cluster, "table_PushUpFilterScalar2", input2);
        pigServer.registerQuery("A = load 'table_PushUpFilterScalar1' as (name, age, gpa);");
        pigServer.registerQuery("B = load 'table_PushUpFilterScalar2' as (name, age);");
        pigServer.registerQuery("C = filter A by age < 20;");
        pigServer.registerQuery("D = filter B by age < 20;");
        pigServer.registerQuery("simple_scalar = limit D 1;");
        pigServer.registerQuery("E = join C by name, D by name;");
        pigServer.registerQuery("F = filter E by C::age==(int)simple_scalar.age;");
        
        Iterator<Tuple> iter = pigServer.openIterator("F");
        
        Tuple t = iter.next();
        Assert.assertTrue(t.toString().equals("(jason,14,4.7,jason,14)"));
        
        Assert.assertFalse(iter.hasNext());
    }

    // See PIG-1683
    @Test
    public void testDuplicateReferenceInnerPlan() throws Exception{
        String[] input1 = {
                "1\t1\t1",
        };
        
        String[] input2 = {
                "1\t1",
                "2\t2"
        };
        
        Util.createInputFile(cluster, "table_testDuplicateReferenceInnerPlan1", input1);
        Util.createInputFile(cluster, "table_testDuplicateReferenceInnerPlan2", input2);
        pigServer.registerQuery("a = load 'table_testDuplicateReferenceInnerPlan1' as (a0, a1, a2);");
        pigServer.registerQuery("b = load 'table_testDuplicateReferenceInnerPlan2' as (b0, b1);");
        pigServer.registerQuery("c = join a by a0, b by b0;");
        pigServer.registerQuery("d = foreach c {d0 = a::a1;d1 = a::a2;generate ((d0 is not null)? d0 : d1);};");
        
        Iterator<Tuple> iter = pigServer.openIterator("d");
        
        Tuple t = iter.next();
        Assert.assertTrue(t.toString().equals("(1)"));
        
        Assert.assertFalse(iter.hasNext());
    }
    
    // See PIG-1719
    @Test
    public void testBinCondSchema() throws IOException {
        String[] inputData = new String[] {"hello world\t2"};
        Util.createInputFile(cluster, "table_testSchemaSerialization.txt", inputData);
        pigServer.registerQuery("a = load 'table_testSchemaSerialization.txt' as (a0:chararray, a1:int);");
        pigServer.registerQuery("b = foreach a generate FLATTEN((a1<=1?{('null')}:TOKENIZE(a0)));");
        pigServer.registerQuery("c = foreach b generate UPPER($0);");
        
        Iterator<Tuple> it = pigServer.openIterator("c");
        Tuple t = it.next();
        Assert.assertTrue(t.get(0).equals("HELLO"));
        t = it.next();
        Assert.assertTrue(t.get(0).equals("WORLD"));
    }
    
    // See PIG-1721
    @Test
    public void testDuplicateInnerAlias() throws Exception{
        String[] input1 = {
                "1\t[key1#5]", "1\t[key2#5]", "2\t[key1#3]"
        };
        
        Util.createInputFile(cluster, "table_testDuplicateInnerAlias", input1);
        pigServer.registerQuery("a = load 'table_testDuplicateInnerAlias' as (a0:int, a1:map[]);");
        pigServer.registerQuery("b = filter a by a0==1;");
        pigServer.registerQuery("c = foreach b { b0 = a1#'key1'; generate ((b0 is null or b0 == '')?1:0);};");
        
        Iterator<Tuple> iter = pigServer.openIterator("c");
        
        Tuple t = iter.next();
        Assert.assertTrue((Integer)t.get(0)==0);
        
        t = iter.next();
        Assert.assertTrue((Integer)t.get(0)==1);
        
        Assert.assertFalse(iter.hasNext());
    }
    
    // See PIG-1729
    @Test
    public void testDereferenceInnerPlan() throws Exception{
        String[] input1 = {
                "1\t2\t3"
        };
        
        String[] input2 = {
                "1\t1"
        };
        
        Util.createInputFile(cluster, "table_testDereferenceInnerPlan1", input1);
        Util.createInputFile(cluster, "table_testDereferenceInnerPlan2", input2);
        pigServer.registerQuery("a = load 'table_testDereferenceInnerPlan1' as (a0:int, a1:int, a2:int);");
        pigServer.registerQuery("b = load 'table_testDereferenceInnerPlan2' as (b0:int, b1:int);");
        pigServer.registerQuery("c = cogroup a by a0, b by b0;");
        pigServer.registerQuery("d = foreach c generate ((COUNT(a)==0L)?null : a.a0) as d0;");
        pigServer.registerQuery("e = foreach d generate flatten(d0);");
        pigServer.registerQuery("f = group e all;");
        
        Iterator<Tuple> iter = pigServer.openIterator("f");
        
        Tuple t = iter.next();
        Assert.assertTrue(t.toString().equals("(all,{(1)})"));
        
        Assert.assertFalse(iter.hasNext());
    }
    
    @Test
    // See PIG-1725
    public void testLOGenerateSchema() throws Exception{
        String[] input1 = {
                "1\t2\t{(1)}",
        };
        
        Util.createInputFile(cluster, "table_testLOGenerateSchema", input1);
        pigServer.registerQuery("a = load 'table_testLOGenerateSchema' as (a0:int, a1, a2:bag{});");
        pigServer.registerQuery("b = foreach a generate a0 as b0, a1 as b1, flatten(a2) as b2:int;");
        pigServer.registerQuery("c = filter b by b0==1;");
        pigServer.registerQuery("d = foreach c generate b0+1, b2;");
        
        Iterator<Tuple> iter = pigServer.openIterator("d");
        
        Tuple t = iter.next();
        Assert.assertTrue(t.toString().equals("(2,1)"));
        
        Assert.assertFalse(iter.hasNext());
    }
    
    // See PIG-1737
    @Test
    public void testMergeSchemaErrorMessage() throws IOException {
        try {
            pigServer.registerQuery("a = load '1.txt' as (a0, a1, a2);");
            pigServer.registerQuery("b = group a by (a0, a1);");
            pigServer.registerQuery("c = foreach b generate flatten(group) as c0;");
            pigServer.openIterator("c");
        } catch (Exception e) {
            PigException pe = LogUtils.getPigException(e);
            Assert.assertTrue(pe.getErrorCode()==1031);
            Assert.assertTrue(pe.getMessage().contains("Incompatable schema"));
            return;
        }
        Assert.fail();
    }
    
    // See PIG-1732
    @Test
    public void testForEachDupColumn() throws Exception{
        String[] input1 = {
                "1\t2",
        };
        
        String[] input2 = {
                "1\t1\t3",
                "2\t4\t2"
        };
        
        Util.createInputFile(cluster, "table_testForEachDupColumn1", input1);
        Util.createInputFile(cluster, "table_testForEachDupColumn2", input2);
        pigServer.registerQuery("a = load 'table_testForEachDupColumn1' as (a0, a1:int);");
        pigServer.registerQuery("b = load 'table_testForEachDupColumn2' as (b0, b1:int, b2);");
        pigServer.registerQuery("c = foreach a generate a0, a1, a1 as a2;");
        pigServer.registerQuery("d = union b, c;");
        pigServer.registerQuery("e = foreach d generate $1;");
        
        Iterator<Tuple> iter = pigServer.openIterator("e");
        
        Tuple t = iter.next();
        Assert.assertTrue(t.size()==1);
        Assert.assertTrue((Integer)t.get(0)==1);
        
        t = iter.next();
        Assert.assertTrue(t.size()==1);
        Assert.assertTrue((Integer)t.get(0)==4);
        
        t = iter.next();
        Assert.assertTrue(t.size()==1);
        Assert.assertTrue((Integer)t.get(0)==2);
        
        Assert.assertFalse(iter.hasNext());
    }
    
    // See PIG-1745
    @Test
    public void testBinStorageByteCast() throws Exception{
        String[] input1 = {
                "1\t2\t3",
        };
        
        Util.createInputFile(cluster, "table_testBinStorageByteCast", input1);
        pigServer.registerQuery("a = load 'table_testBinStorageByteCast' as (a0, a1, a2);");
        pigServer.store("a", "table_testBinStorageByteCast.temp", BinStorage.class.getName());
        
        pigServer.registerQuery("a = load 'table_testBinStorageByteCast.temp' using BinStorage() as (a0, a1, a2);");
        pigServer.registerQuery("b = foreach a generate (long)a0;");
        
        try {
            pigServer.openIterator("b");
        } catch (Exception e) {
            PigException pe = LogUtils.getPigException(e);
            Assert.assertTrue(pe.getErrorCode()==1118);
            return;
        }
        
        Assert.fail();
    }
    
    // See PIG-1761
    @Test
    public void testBagDereferenceInMiddle1() throws Exception{
        String[] input1 = {
                "foo@apache#44",
        };
        
        Util.createInputFile(cluster, "table_testBagDereferenceInMiddle1", input1);
        pigServer.registerQuery("a = load 'table_testBagDereferenceInMiddle1' as (a0:chararray);");
        pigServer.registerQuery("b = foreach a generate UPPER(REGEX_EXTRACT_ALL(a0, '.*@(.*)#.*').$0);");
        
        Iterator<Tuple> iter = pigServer.openIterator("b");
        Tuple t = iter.next();
        Assert.assertTrue(t.size()==1);
        Assert.assertTrue(t.get(0).equals("APACHE"));
    }
    
    // See PIG-1843
    @Test
    public void testBagDereferenceInMiddle2() throws Exception{
        String[] input1 = {
                "foo apache",
        };
        
        Util.createInputFile(cluster, "table_testBagDereferenceInMiddle2", input1);
        pigServer.registerQuery("a = load 'table_testBagDereferenceInMiddle2' as (a0:chararray);");
        pigServer.registerQuery("b = foreach a generate " + MapGenerate.class.getName() + " (STRSPLIT(a0).$0);");
        
        Iterator<Tuple> iter = pigServer.openIterator("b");
        Tuple t = iter.next();
        Assert.assertTrue(t.size()==1);
        Assert.assertTrue(t.toString().equals("([key#1])"));
    }
    
    // See PIG-1766
    @Test
    public void testForEachSameOriginColumn1() throws Exception{
        String[] input1 = {
                "1\t2",
                "1\t3",
                "2\t4",
                "2\t5",
        };
        
        String[] input2 = {
                "1\tone",
                "2\ttwo",
        };
        
        Util.createInputFile(cluster, "table_testForEachSameOriginColumn1_1", input1);
        Util.createInputFile(cluster, "table_testForEachSameOriginColumn1_2", input2);
        pigServer.registerQuery("A = load 'table_testForEachSameOriginColumn1_1' AS (a0:int, a1:int);");
        pigServer.registerQuery("B = load 'table_testForEachSameOriginColumn1_2' AS (b0:int, b1:chararray);");
        pigServer.registerQuery("C = join A by a0, B by b0;");
        pigServer.registerQuery("D = foreach B generate b0 as d0, b1 as d1;");
        pigServer.registerQuery("E = join C by a1, D by d0;");
        pigServer.registerQuery("F = foreach E generate b1, d1;");
        
        Iterator<Tuple> iter = pigServer.openIterator("F");
        Tuple t = iter.next();
        Assert.assertTrue(t.size()==2);
        Assert.assertTrue(t.get(0).equals("one"));
        Assert.assertTrue(t.get(1).equals("two"));
    }
    
    // See PIG-1771
    @Test
    public void testLoadWithDifferentSchema() throws Exception{
        String[] input1 = {
                "hello\thello\t(hello)\t[key#value]",
        };
        
        Util.createInputFile(cluster, "table_testLoadWithDifferentSchema1", input1);
        pigServer.registerQuery("a = load 'table_testLoadWithDifferentSchema1' as (a0:chararray, a1:chararray, a2, a3:map[]);");
        pigServer.store("a", "table_testLoadWithDifferentSchema1.bin", "org.apache.pig.builtin.BinStorage");
        
        pigServer.registerQuery("b = load 'table_testLoadWithDifferentSchema1.bin' USING BinStorage('Utf8StorageConverter') AS (b0:chararray, b1:chararray, b2:tuple(), b3:map[]);");
        Iterator<Tuple> iter = pigServer.openIterator("b");
        
        Tuple t = iter.next();
        Assert.assertTrue(t.size()==4);
        Assert.assertTrue(t.toString().equals("(hello,hello,(hello),[key#value])"));
    }
    
    static public class MapGenerate extends EvalFunc<Map<String, Integer>> {
        @Override
        public Map<String, Integer> exec(Tuple input) throws IOException {
            Map<String, Integer> m = new HashMap<String, Integer>();
            m.put("key", new Integer(input.size()));
            return m;
        }
        
        @Override
        public Schema outputSchema(Schema input) {
            return new Schema(new Schema.FieldSchema(getSchemaName("parselong", input), DataType.MAP));
        }
    }
    
    // See PIG-1277
    @Test
    public void testWrappingUnknownKey1() throws Exception{
        String[] input1 = {
                "1",
        };
        
        Util.createInputFile(cluster, "table_testWrappingUnknownKey1", input1);

        pigServer.registerQuery("a = load 'table_testWrappingUnknownKey1' as (a0);");
        pigServer.registerQuery("b = foreach a generate a0, "+ MapGenerate.class.getName() + "(*) as m:map[];");
        pigServer.registerQuery("c = foreach b generate a0, m#'key' as key;");
        pigServer.registerQuery("d = group c by key;");
        
        Iterator<Tuple> iter = pigServer.openIterator("d");
        
        Tuple t = iter.next();
        Assert.assertTrue(t.size()==2);
        Assert.assertTrue(t.toString().equals("(1,{(1,1)})"));
        Assert.assertFalse(iter.hasNext());
    }
    
    // See PIG-999
    @Test
    public void testWrappingUnknownKey2() throws Exception{
        String[] input1 = {
                "1",
        };
        
        Util.createInputFile(cluster, "table_testWrappingUnknownKey2", input1);

        pigServer.registerQuery("a = load 'table_testWrappingUnknownKey2' as (a0);");
        pigServer.registerQuery("b = foreach a generate a0, "+ MapGenerate.class.getName() + "(*) as m:map[];");
        pigServer.registerQuery("c = foreach b generate a0, m#'key' as key;");
        pigServer.registerQuery("d = order c by key;");
        
        Iterator<Tuple> iter = pigServer.openIterator("d");
        
        Tuple t = iter.next();
        Assert.assertTrue(t.size()==2);
        Assert.assertTrue(t.toString().equals("(1,1)"));
        Assert.assertFalse(iter.hasNext());
    }
    
    // See PIG-1065
    @Test
    public void testWrappingUnknownKey3() throws Exception{
        String[] input1 = {
                "1\t2",
                "2\t3"
        };
        
        String[] input2 = {
                "1",
        };
        
        Util.createInputFile(cluster, "table_testWrappingUnknownKey3_1", input1);
        Util.createInputFile(cluster, "table_testWrappingUnknownKey3_2", input2);

        pigServer.registerQuery("a = load 'table_testWrappingUnknownKey3_1' as (a0:chararray, a1:chararray);");
        pigServer.registerQuery("b = load 'table_testWrappingUnknownKey3_2' as (b0:chararray);");
        pigServer.registerQuery("c = union a, b;");
        pigServer.registerQuery("d = order c by $0;");
        
        Collection<String> results = new HashSet<String>();
        results.add("(1,2)");
        results.add("(1)");
        results.add("(2,3)");
        
        Iterator<Tuple> iter = pigServer.openIterator("d");
        
        Tuple t = iter.next();
        Assert.assertTrue(results.contains(t.toString()));
        t = iter.next();
        Assert.assertTrue(results.contains(t.toString()));
        t = iter.next();
        Assert.assertTrue(results.contains(t.toString()));
        Assert.assertFalse(iter.hasNext());
    }
    
    // See PIG-1787
    @Test
    public void testOrderByLimitJoin() throws Exception{
        String[] input1 = {
                "1\t1",
                "1\t2"
        };
        
        Util.createInputFile(cluster, "table_testOrderByLimitJoin", input1);

        pigServer.registerQuery("a = load 'table_testOrderByLimitJoin' as (a0, a1);");
        pigServer.registerQuery("b = group a by a0;");
        pigServer.registerQuery("c = foreach b generate group as c0, COUNT(a) as c1;");
        pigServer.registerQuery("d = order c by c1 parallel 2;");
        pigServer.registerQuery("e = limit d 10;");
        pigServer.registerQuery("f = join e by c0, a by a0;");
        
        Iterator<Tuple> iter = pigServer.openIterator("f");
        
        Tuple t = iter.next();
        Assert.assertTrue(t.toString().equals("(1,2,1,1)"));
        t = iter.next();
        Assert.assertTrue(t.toString().equals("(1,2,1,2)"));
        Assert.assertFalse(iter.hasNext());
    }
    
    // See PIG-1785
    @Test
    public void testForEachSameOriginColumn2() throws Exception{
        String[] input1 = {
                "{(1,2),(2,3)}",
        };
        
        Util.createInputFile(cluster, "table_testForEachSameOriginColumn2", input1);

        pigServer.registerQuery("a = load 'table_testForEachSameOriginColumn2' as (a0:bag{t:tuple(i0:int, i1:int)});");
        pigServer.registerQuery("b = foreach a generate flatten(a0) as (b0, b1), flatten(a0) as (b2, b3);");
        pigServer.registerQuery("c = filter b by b0>b2;");
        
        Iterator<Tuple> iter = pigServer.openIterator("c");
        
        Tuple t = iter.next();
        Assert.assertTrue(t.toString().contains("(2,3,1,2)"));
        Assert.assertFalse(iter.hasNext());
    }
    
    // See PIG-1785
    @Test
    public void testForEachSameOriginColumn3() throws Exception{
        String[] input1 = {
                "1\t1\t2",
                "1\t2\t3",
        };
        
        Util.createInputFile(cluster, "table_testForEachSameOriginColumn3", input1);

        pigServer.registerQuery("a = load 'table_testForEachSameOriginColumn3' as (a0:int, a1:int, a2:int);");
        pigServer.registerQuery("b = group a by a0;");
        pigServer.registerQuery("c = foreach b generate flatten(a.(a1,a2)) as (b0, b1), flatten(a.(a1,a2)) as (b2, b3);");
        pigServer.registerQuery("d = filter c by b0>b2;");
        
        Iterator<Tuple> iter = pigServer.openIterator("d");
        
        Tuple t = iter.next();
        Assert.assertTrue(t.toString().contains("(2,3,1,2)"));
        Assert.assertFalse(iter.hasNext());
    }
    
    // See PIG-1785
    @Test
    public void testAddingTwoBag() {
        try {
            pigServer.registerQuery("a = load '1.txt' as (name:chararray, age:int, gpa:double);");
            pigServer.registerQuery("b = group a by name;");
            pigServer.registerQuery("c = foreach b generate group, SUM(a.age*a.gpa);");
            pigServer.openIterator("c");
        } catch (Exception e) {
            PigException pe = LogUtils.getPigException(e);
            Assert.assertTrue(pe.getErrorCode()==1039);
            Assert.assertTrue(pe.getMessage().contains("incompatible types"));
            return;
        }
        Assert.fail();
    }
    
    public static class BagGenerateNoSchema extends EvalFunc<DataBag> {
        @Override
        public DataBag exec(Tuple input) throws IOException {
            DataBag bg = DefaultBagFactory.getInstance().newDefaultBag();
            bg.add(input);
            return bg;
        }
    }
    
    // See PIG-1813
    @Test
    public void testUDFNoSchemaPropagate1() throws Exception{
        String[] input1 = {
                "[key#1,key2#2]",
                "[key#2,key2#3]",
        };
        
        Util.createInputFile(cluster, "table_testUDFNoSchemaPropagate1", input1);

        pigServer.registerQuery("a = load 'table_testUDFNoSchemaPropagate1' as (a0:map[]);");
        pigServer.registerQuery("b = foreach a generate " + BagGenerateNoSchema.class.getName() + "(*) as b0;");
        pigServer.registerQuery("c = foreach b generate flatten(IdentityColumn(b0));");
        pigServer.registerQuery("d = foreach c generate $0#'key';");
        
        Iterator<Tuple> iter = pigServer.openIterator("d");
        
        Tuple t = iter.next();
        Assert.assertTrue(t.toString().contains("(1)"));
        t = iter.next();
        Assert.assertTrue(t.toString().contains("(2)"));
        Assert.assertFalse(iter.hasNext());
    }
    
    // See PIG-1813
    @Test
    public void testUDFNoSchemaPropagate2() throws Exception{
        String[] input1 = {
                "[key#1,key2#2]",
                "[key#2,key2#3]",
        };
        
        Util.createInputFile(cluster, "table_testUDFNoSchemaPropagate2", input1);

        pigServer.registerQuery("a = load 'table_testUDFNoSchemaPropagate2' as (a0:map[]);");
        pigServer.registerQuery("b = foreach a generate flatten(" + BagGenerateNoSchema.class.getName() + "(*)) as b0;");
        pigServer.registerQuery("c = foreach b generate IdentityColumn(b0);");
        pigServer.registerQuery("d = foreach c generate $0#'key';");
        
        Iterator<Tuple> iter = pigServer.openIterator("d");
        
        Tuple t = iter.next();
        Assert.assertTrue(t.toString().contains("(1)"));
        t = iter.next();
        Assert.assertTrue(t.toString().contains("(2)"));
        Assert.assertFalse(iter.hasNext());
    }
    
    // See PIG-1812
    @Test
    public void testLocalRearrangeInReducer() throws Exception{
        String[] input1 = {
                "1\t1",
                "1\t1",
                "1\t2",
        };
        
        String[] input2 = {
                "1\t1",
        };

        Util.createInputFile(cluster, "table_testLocalRearrangeInReducer1", input1);
        Util.createInputFile(cluster, "table_testLocalRearrangeInReducer2", input2);

        pigServer.registerQuery("a = load 'table_testLocalRearrangeInReducer1' as (a0, a1);");
        pigServer.registerQuery("b = distinct a;");
        pigServer.registerQuery("c = load 'table_testLocalRearrangeInReducer2' as (c0, c1);");
        pigServer.registerQuery("d = cogroup b by a0, c by c0;");
        pigServer.registerQuery("e = foreach d { e1 = order c by c1; generate e1;};");
        
        Iterator<Tuple> iter = pigServer.openIterator("e");
        
        Tuple t = iter.next();
        Assert.assertTrue(t.toString().contains("({(1,1)})"));
        Assert.assertFalse(iter.hasNext());
    }
    
    // See PIG-1850
    @Test
    public void testProjectNullSchema() throws Exception{
        String[] input = {
                "0\t1",
        };
        
        Util.createInputFile(cluster, "table_testProjectNullSchema", input);

        pigServer.registerQuery("a = load 'table_testProjectNullSchema';");
        pigServer.registerQuery("b = foreach a generate ASIN($0), $1;");
        pigServer.registerQuery("c = order b by $0;");
        
        Iterator<Tuple> iter = pigServer.openIterator("c");
        
        Tuple t = iter.next();
        Assert.assertTrue(t.toString().contains("(0.0,1)"));
        Assert.assertFalse(iter.hasNext());
    }
}
