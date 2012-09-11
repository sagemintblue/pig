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

package org.apache.pig;

/**
 * Container for static configuration strings, defaults, etc.
 */
public class PigConfiguration {

    /**
     * Controls the fraction of total memory that is allowed to be used by
     * cached bags. Default is 0.2.
     */
    public static final String PROP_CACHEDBAG_MEMUSAGE = "pig.cachedbag.memusage";

    /**
     * Controls whether partial aggregation is turned on
     */
    public static final String PROP_EXEC_MAP_PARTAGG = "pig.exec.mapPartAgg";

    /**
     * Controls the minimum reduction in-mapper Partial Aggregation should achieve in order
     * to stay on. If after a period of observation this reduction is not achieved,
     * in-mapper aggregation will be turned off and a message logged to that effect.
     */
    public static final String PARTAGG_MINREDUCTION = "pig.exec.mapPartAgg.minReduction";

    /**
     * Controls whether execution time of Pig UDFs should be tracked.
     * This feature uses counters; use judiciously.
     */
    public static final String TIME_UDFS_PROP = "pig.udf.profile";

    /**
     * Turns off use of combiners in MapReduce jobs produced by Pig.
     */
    public static final String PROP_NO_COMBINER = "pig.exec.nocombiner";
}
