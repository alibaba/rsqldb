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
package com.alibaba.rsqldb.parser.model.statement;

public enum SQLType {
    //---------------------------------insert------------------------------
    INSERT_VALUE,
    INSERT_QUERY,
    //---------------------------------create------------------------------
    CREATE_VIEW,
    CREATE_TABLE,

    //---------------------------------select------------------------------
    SELECT_FROM,
    SELECT_FROM_WHERE,

    //GROUPBY
    SELECT_FROM_GROUPBY,
    SELECT_FROM_WHERE_GROUPBY,

    //HAVING
    SELECT_FROM_GROUPBY_HAVING,
    SELECT_FROM_WHERE_GROUPBY_HAVING,

    //WINDOW
    SELECT_FROM_GROUPBY_WINDOW,
    SELECT_FROM_WHERE_GROUPBY_WINDOW,

    SELECT_FROM_GROUPBY_WINDOW_HAVING,
    SELECT_FROM_WHERE_GROUPBY_WINDOW_HAVING,

    //JOIN
    SELECT_FROM_JOIN,

    SELECT_FROM_WHERE_JOIN,
    SELECT_FROM_WHERE_JOIN_WHERE,
    SELECT_FROM_WHERE_JOIN_WHERE_GROUPBY,
    SELECT_FROM_WHERE_JOIN_WHERE_GROUPBY_HAVING,

    SELECT_FROM_WHERE_JOIN_GROUPBY,
    SELECT_FROM_WHERE_JOIN_GROUPBY_HAVING,

    SELECT_FROM_JOIN_WHERE,
    SELECT_FROM_JOIN_WHERE_GROUPBY,
    SELECT_FROM_JOIN_WHERE_GROUPBY_HAVING,

    SELECT_FROM_JOIN_GROUPBY,
    /**
     * SELECT Websites.name, Websites.url, SUM(access_log.count) AS nums FROM access_log INNER JOIN Websites ON access_log.site_id=Websites.id
     * GROUP BY Websites.name
     * HAVING SUM(access_log.count) > 200;
     */
    SELECT_FROM_JOIN_GROUPBY_HAVING,


    //JOIN 和WINDOW组合的sql暂时不支持
}
