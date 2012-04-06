package org.apache.pig;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.*;

import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.builtin.PigStorage;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.util.UDFContext;

public class DebugPigStorage extends PigStorage {
    private static final String KEY = "dummy.prop.key";
    private static final String VALUE = "1234";

    private String udfcSignature = null;

    public DebugPigStorage() { super(); }

    public DebugPigStorage(String delimiter) {
        super(delimiter);
    }

    public DebugPigStorage(String delimiter, String options) {
        super(delimiter, options);
    }

    // **************** LoadFunc methods ****************

    @Override
    public String relativeToAbsolutePath(String location, Path curDir) throws IOException {
        logCalled();
        return super.relativeToAbsolutePath(location, curDir);
    }

    @Override
    public void setUDFContextSignature(String signature) {
        logCalled();
        this.udfcSignature = signature;
        super.setUDFContextSignature(signature);
    }

    @Override
    public void setLocation(String location, Job job) throws IOException {
        logCalled();
        super.setLocation(location, job);
    }

    @Override
    public InputFormat getInputFormat() {
        logCalled();
        return super.getInputFormat();
    }

    @Override
    public void prepareToRead(RecordReader reader, PigSplit split) {
        logCalled();
        super.prepareToRead(reader, split);
    }

    @Override
    public Tuple getNext() throws IOException {
        logCalled();
        return super.getNext();
    }

    @Override
    public LoadCaster getLoadCaster() throws IOException {
        logCalled();
        return super.getLoadCaster();
    }

    // **************** OrderedLoadFunc method ****************

    @Override
    public WritableComparable<?> getSplitComparable(InputSplit split) throws IOException {
        logCalled();
        return super.getSplitComparable(split);
    }

    // **************** LoadMetadata method ****************

    @Override
    public ResourceSchema getSchema(String location, Job job) throws IOException {
        logCalled();
        return super.getSchema(location, job);
    }

    @Override
    public ResourceStatistics getStatistics(String location, Job job) throws IOException {
        logCalled();
        return super.getStatistics(location, job);
    }

    @Override
    public String[] getPartitionKeys(String location, Job job) throws IOException {
        logCalled();
        return super.getPartitionKeys(location, job);
    }

    @Override
    public void setPartitionFilter(Expression partitionFilter) throws IOException {
        logCalled();
        super.setPartitionFilter(partitionFilter);
    }

    // **************** StoreFuncInterface method ****************

    @Override
    public void setStoreFuncUDFContextSignature(String signature) {
        logCalled();
        super.setStoreFuncUDFContextSignature(signature);
    }

    @Override
    public String relToAbsPathForStoreLocation(String location, Path curDir) throws IOException {
        logCalled();
        return super.relToAbsPathForStoreLocation(location, curDir);
    }

    // only called if schema is defined
    @Override
    public void checkSchema(ResourceSchema s) throws IOException {
        logCalled();
        super.checkSchema(s);
        setValue();
    }

    @Override
    public void setStoreLocation(String location, Job job) throws IOException {
        logCalled();
        super.setStoreLocation(location, job);
    }

    @Override
    public OutputFormat getOutputFormat() {
        logCalled();
        assertValue();
        return super.getOutputFormat();
    }

    @Override
    public void prepareToWrite(RecordWriter writer) {
        logCalled();
        super.prepareToWrite(writer);
        assertValue();
    }

    @Override
    public void putNext(Tuple f) throws IOException {
        logCalled();
        super.putNext(f);
    }

    @Override
    public void cleanupOnFailure(String location, Job job) throws IOException {
        logCalled();
        super.cleanupOnFailure(location, job);
    }

    // **************** StoreMetadata methods ****************

    @Override
    public void storeSchema(ResourceSchema schema, String location, Job job) throws IOException {
        logCalled();
        super.storeSchema(schema, location, job);
    }

    @Override
    public void storeStatistics(ResourceStatistics stats, String location, Job job) throws IOException {
        logCalled();
        super.storeStatistics(stats, location, job);
    }

    // **************** LoadPushDown methods ****************
    @Override
    public List<OperatorSet> getFeatures() {
        logCalled();
        return super.getFeatures();
    }

    @Override
    public RequiredFieldResponse pushProjection(RequiredFieldList requiredFieldList) throws FrontendException {
        logCalled();
        return super.pushProjection(requiredFieldList);
    }

    @Override
    public boolean equals(Object obj) {
        logCalled();
        return super.equals(obj);
    }

    @Override
    public boolean equals(PigStorage other) {
        logCalled();
        return super.equals(other);
    }

    private Properties getProps() {
        UDFContext udfc = UDFContext.getUDFContext();
        Properties p = udfc.getUDFProperties(this.getClass(), new String[] {udfcSignature});
        mLog.info("UDFContext properties: " + p);
        return p;
    }
    
    private void setValue() {
        mLog.info("setting " + KEY + " to " + VALUE);
        getProps().setProperty(KEY, VALUE);
    }
    
    private void assertValue() {
        if (!VALUE.equals(getProps().getProperty(KEY))) {
            throw new RuntimeException("prepareToWrite - prop not found");
        }
        mLog.info("Value found");
    }

    private void logCalled() {
        mLog.info("Call sequence: " + getMethodCallLocation(4, true, true) +
                " -> " + getMethodCallLocation(3, true, true) +
                " -> " + getMethodCallLocation(2, true, true) +
                " -> " + getObjectId() + " " + getMethodCallLocation(1, false, false));
    }

    static List<Object> objectHeap = new ArrayList<Object>();

    private int getObjectId() {
        int objectId = 0;
        for(Object obj : objectHeap) {
            if (this == obj) { break; }
            objectId++;
        }
        if (objectId == objectHeap.size()) { objectHeap.add(this); }

        return objectId;
    }

    /**
     * Returns a method in the call stack at the given depth. Depth 0 will return the method that
     * called this getMethodName, depth 1 the method that called it, etc...
     * @param depth
     * @return
     */
    protected String getMethodCallLocation(final int depth, boolean showClass, boolean showLineNumber) {
        final StackTraceElement[] ste = Thread.currentThread().getStackTrace();
        int index =  2 + depth;

        if (index >= ste.length) { return ""; }

        StringBuilder sb = new StringBuilder();
        if (showClass) {
            sb.append(ste[index].getClassName()).append(":");
        }
        sb.append(ste[index].getMethodName()).append("()");

        if (showLineNumber) {
            sb.append(":").append(ste[index].getLineNumber());
        }

        return sb.toString();
    }
}
