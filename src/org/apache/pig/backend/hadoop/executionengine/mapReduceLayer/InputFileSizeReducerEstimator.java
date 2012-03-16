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
package org.apache.pig.backend.hadoop.executionengine.mapReduceLayer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.pig.LoadFunc;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POLoad;
import org.apache.pig.impl.util.UriUtil;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Class that estimates the number of reducers based on the total size of the
 * input data.  Two parameters can been configured for the estimation, one is
 * pig.exec.reducers.max which constrains the maximum number of reducer tasks
 * (default is 999). The other is pig.exec.reducers.bytes.per.reducer
 * (default value is 1000*1000*1000) which sets how much data to be handled by
 * each reducer. e.g. if the following is your pig script
 * <pre>
 * a = load '/data/a';
 * b = load '/data/b';
 * c = join a by $0, b by $0;
 * store c into '/tmp';
 * </pre>
 * and the size of /data/a is 1000*1000*1000, and the size of /data/b is
 * 2*1000*1000*1000 then the estimated number of reducer to use will be
 * (1000*1000*1000+2*1000*1000*1000)/(1000*1000*1000)=3
 *
 */
public class InputFileSizeReducerEstimator implements PigReducerEstimator {
    private static final Log log = LogFactory.getLog(InputFileSizeReducerEstimator.class);

    private static final String BYTES_PER_REDUCER_PARAM = "pig.exec.reducers.bytes.per.reducer";
    private static final String MAX_REDUCER_COUNT_PARAM = "pig.exec.reducers.max";

    private static final long DEFAULT_BYTES_PER_REDUCER = 1000 * 1000 * 1000;
    private static final int DEFAULT_MAX_REDUCER_COUNT_PARAM = 999;

    /**
     * Determines the number of reducers to be used.
     *
     * @param conf
     * @param lds
     * @throws java.io.IOException
     */
    @Override
    public int estimateNumberOfReducers(Configuration conf, List<POLoad> lds) throws IOException {
        long bytesPerReducer = conf.getLong(BYTES_PER_REDUCER_PARAM, DEFAULT_BYTES_PER_REDUCER);
        int maxReducers = conf.getInt(MAX_REDUCER_COUNT_PARAM, DEFAULT_MAX_REDUCER_COUNT_PARAM);
        long totalInputFileSize = getTotalInputFileSize(conf, lds);

        log.info("BytesPerReducer=" + bytesPerReducer + " maxReducers="
            + maxReducers + " totalInputFileSize=" + totalInputFileSize);

        int reducers = (int)Math.ceil((totalInputFileSize+0.0) / bytesPerReducer);
        reducers = Math.max(1, reducers);
        reducers = Math.min(maxReducers, reducers);
        conf.setInt("mapred.reduce.tasks", reducers);

        log.info("Neither PARALLEL nor default parallelism is set for this job. Setting number of reducers to " + reducers);
        return reducers;
    }

    private static long getTotalInputFileSize(Configuration conf, List<POLoad> lds) throws IOException {
        List<String> inputs = new ArrayList<String>();
        if(lds!=null && lds.size()>0){
            for (POLoad ld : lds) {
                inputs.add(ld.getLFile().getFileName());
            }
        }
        long size = 0;

        for (String input : inputs){
            //Using custom uri parsing because 'new Path(location).toUri()' fails
            // for some valid uri's (eg jdbc style), and 'new Uri(location)' fails
            // for valid hdfs paths that contain curly braces
            if(!UriUtil.isHDFSFileOrLocalOrS3N(input)){
                //skip  if it is not hdfs or local file or s3n
                continue;
            }

            //the input file location might be a list of comma separeated files,
            // separate them out
            for(String location : LoadFunc.getPathStrings(input)){
                if(! UriUtil.isHDFSFileOrLocalOrS3N(location)){
                    continue;
                }
                Path path = new Path(location);
                FileSystem fs = path.getFileSystem(conf);
                FileStatus[] status=fs.globStatus(path);
                if (status != null){
                    for (FileStatus s : status){
                        size += getPathLength(fs, s);
                    }
                }
            }
        }
        return size;
    }

    private static long getPathLength(FileSystem fs, FileStatus status) throws IOException{
        if (!status.isDir()){
            return status.getLen();
        }else{
            FileStatus[] children = fs.listStatus(status.getPath());
            long size=0;
            for (FileStatus child : children){
                size +=getPathLength(fs, child);
            }
            return size;
        }
    }
}
