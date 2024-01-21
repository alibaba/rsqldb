/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.rsqldb.clients;

import org.apache.rocketmq.streams.common.topology.graph.JobGraph;
import org.junit.Before;
import org.junit.Test;

public class SQLTest {

    private SQLExecutionEnvironment sqlExecutionEnvironment;

    @Before
    public void init() {
        sqlExecutionEnvironment = SQLExecutionEnvironment.getExecutionEnvironment().db("", "", "", "");

    }

    /**
     * insert into t_r_device_sink
     * select
     * tenantCode,
     * gateway,
     * 'D_100'  as nodeCode,
     * 'C_1400_V_1400_sxx_xg_cs' as measurePoint,
     * getValueChangeTimes(fullName,`value`)  as `value`,
     * 'Float' as valueType,
     * HOP_END(`timestamp`,INTERVAL '60' SECOND,INTERVAL '600' SECOND) as `timestamp`,
     * '192' as quality
     * from t_r_point_source_view
     * where fullName in ('tenant_industry_brain/default_gateway_code/N_2/cv_ll','tenant_industry_brain/default_gateway_code/N_2/cv_hl')
     * group by HOP(`timestamp`,INTERVAL '60' SECOND,INTERVAL '600' SECOND),tenantCode,gateway;
     * from table |count(*) as c group by name| sum(c) as |select a,b|insert into table2|
     * a join b on a.x=b.x|select a.*
     * <p>
     * from table | where a>x and b>x | count(x)>0 group by |as a;
     * from table2| where a> and b>x| count(x) group by| as c;
     * a join c on a.x=c.y|where x>0|insert into e;
     *
     * @throws InterruptedException
     */
    @Test
    public void testSQL() throws InterruptedException {
        System.out.println("ok");
        JobGraph jobGraph=SQLExecutionEnvironment.getExecutionEnvironment().createJobFromPath("tmp","classpath://kafka.sql");
        jobGraph.start();

        Thread.sleep(10000000000l);
    }
}
