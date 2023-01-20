/*
 * Copyright 1999-2018 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.rsqldb.parser.model.statement.query;

import com.alibaba.rsqldb.parser.SqlParser;
import com.alibaba.rsqldb.common.exception.SyntaxErrorException;
import com.alibaba.rsqldb.parser.model.statement.SQLType;

import java.util.List;

public class SelectTypeUtil {
    private static int wherePhraseNums;
    private static int groupByPhraseNums;
    private static int havingPhraseNums;
    private static boolean hasBeforeWhere = false;
    private static boolean hasAfterWhere = false;
    private static boolean windowFunction = false;
    private static boolean join = false;

    public static SQLType whichType(SqlParser.QueryContext ctx) {
        valid(ctx);

        if (!join) {
            if (wherePhraseNums == 0 && groupByPhraseNums == 0 && havingPhraseNums == 0) {
                return SQLType.SELECT_FROM;
            }
            if (wherePhraseNums == 1 && groupByPhraseNums == 0 && havingPhraseNums == 0) {
                return SQLType.SELECT_FROM_WHERE;
            }
            if (wherePhraseNums == 0 && groupByPhraseNums == 1 && havingPhraseNums == 0 && !windowFunction) {
                return SQLType.SELECT_FROM_GROUPBY;
            }
            if (wherePhraseNums == 1 && groupByPhraseNums == 1 && havingPhraseNums == 0 && !windowFunction) {
                return SQLType.SELECT_FROM_WHERE_GROUPBY;
            }
            if (wherePhraseNums == 0 && groupByPhraseNums == 1 && havingPhraseNums == 1 && !windowFunction) {
                return SQLType.SELECT_FROM_GROUPBY_HAVING;
            }
            if (wherePhraseNums == 1 && groupByPhraseNums == 1 && havingPhraseNums == 1 && !windowFunction) {
                return SQLType.SELECT_FROM_WHERE_GROUPBY_HAVING;
            }

            //window
            if (wherePhraseNums == 0 && windowFunction && havingPhraseNums == 0) {
                return SQLType.SELECT_FROM_GROUPBY_WINDOW;
            }
            if (wherePhraseNums == 1 && windowFunction && havingPhraseNums == 0) {
                return SQLType.SELECT_FROM_WHERE_GROUPBY_WINDOW;
            }
            if (wherePhraseNums == 0 && windowFunction && havingPhraseNums == 1) {
                return SQLType.SELECT_FROM_GROUPBY_WINDOW_HAVING;
            }
            if (wherePhraseNums == 1 && windowFunction && havingPhraseNums == 1) {
                return SQLType.SELECT_FROM_WHERE_GROUPBY_WINDOW_HAVING;
            }
        } else {
            if (wherePhraseNums == 0 && groupByPhraseNums == 0 && havingPhraseNums == 0) {
                return SQLType.SELECT_FROM_JOIN;
            }
            if (hasBeforeWhere && wherePhraseNums == 1 && groupByPhraseNums == 0 && havingPhraseNums == 0) {
                return SQLType.SELECT_FROM_WHERE_JOIN;
            }
            if (wherePhraseNums == 2 && groupByPhraseNums == 0 && havingPhraseNums == 0) {
                return SQLType.SELECT_FROM_WHERE_JOIN_WHERE;
            }
            if (wherePhraseNums == 2 && groupByPhraseNums == 1 && havingPhraseNums == 0) {
                return SQLType.SELECT_FROM_WHERE_JOIN_WHERE_GROUPBY;
            }
            if (wherePhraseNums == 2 && groupByPhraseNums == 1 && havingPhraseNums == 1) {
                return SQLType.SELECT_FROM_WHERE_JOIN_WHERE_GROUPBY_HAVING;
            }

            if (hasBeforeWhere && wherePhraseNums == 1 && groupByPhraseNums == 1 && havingPhraseNums == 0) {
                return SQLType.SELECT_FROM_WHERE_JOIN_GROUPBY;
            }
            if (hasBeforeWhere && wherePhraseNums == 1 && groupByPhraseNums == 1 && havingPhraseNums == 1) {
                return SQLType.SELECT_FROM_WHERE_JOIN_GROUPBY_HAVING;
            }
            if (hasAfterWhere && wherePhraseNums == 1 && groupByPhraseNums == 0 && havingPhraseNums == 0) {
                return SQLType.SELECT_FROM_JOIN_WHERE;
            }
            if (hasAfterWhere && wherePhraseNums == 1 && groupByPhraseNums == 1 && havingPhraseNums == 0) {
                return SQLType.SELECT_FROM_JOIN_WHERE_GROUPBY;
            }
            if (hasAfterWhere && wherePhraseNums == 1 && groupByPhraseNums == 1 && havingPhraseNums == 1) {
                return SQLType.SELECT_FROM_JOIN_WHERE_GROUPBY_HAVING;
            }

            if (hasBeforeWhere && wherePhraseNums == 0 && groupByPhraseNums == 1 && havingPhraseNums == 0) {
                return SQLType.SELECT_FROM_JOIN_GROUPBY;
            }

            if (wherePhraseNums == 0 && groupByPhraseNums == 1 && havingPhraseNums == 1) {
                return SQLType.SELECT_FROM_JOIN_GROUPBY_HAVING;
            }

        }


        throw new UnsupportedOperationException();
    }


    //判断groupBy、 having是否在join前，是的话报错。
    private static void valid(SqlParser.QueryContext ctx) {
        reset();

        SqlParser.JoinPhraseContext joinPhraseContext = ctx.joinPhrase();
        int joinStartIndex = Integer.MIN_VALUE;
        if (joinPhraseContext != null) {
            join = true;
            joinStartIndex = joinPhraseContext.start.getStartIndex();
        }

        {
            List<SqlParser.GroupByPhraseContext> groupByPhraseContexts = ctx.groupByPhrase();
            if (groupByPhraseContexts != null && groupByPhraseContexts.size() != 0) {
                if (groupByPhraseContexts.size() != 1) {
                    throw new SyntaxErrorException("groupBy phrase nums more than 1");
                }

                groupByPhraseNums = 1;
                SqlParser.GroupByPhraseContext groupByPhraseContext = groupByPhraseContexts.get(0);
                int groupByStartIndex = groupByPhraseContext.start.getStartIndex();
                if (groupByStartIndex < joinStartIndex) {
                    throw new SyntaxErrorException("groupBy statement should come after join.");
                }

                SqlParser.WindowFunctionContext windowFunctionContext = groupByPhraseContext.windowFunction();
                if (windowFunctionContext != null) {
                    windowFunction = true;
                }
            } else {
                groupByPhraseNums = 0;
            }
        }

        {
            List<SqlParser.HavingPhraseContext> havingPhraseContexts = ctx.havingPhrase();
            if (havingPhraseContexts != null && havingPhraseContexts.size() != 0) {
                if (havingPhraseContexts.size() != 1) {
                    throw new SyntaxErrorException("having phrase nums more than 1");
                }
                havingPhraseNums = 1;
                SqlParser.HavingPhraseContext havingPhraseContext = havingPhraseContexts.get(0);
                int havingStartIndex = havingPhraseContext.start.getStartIndex();
                if (havingStartIndex < joinStartIndex) {
                    throw new SyntaxErrorException("having statement should come after join.");
                }
            } else {
                havingPhraseNums = 0;
            }
        }


        {
            List<SqlParser.WherePhraseContext> wherePhraseContexts = ctx.wherePhrase();
            if (wherePhraseContexts != null) {
                wherePhraseNums = wherePhraseContexts.size();
                if (wherePhraseNums > 2) {
                    throw new SyntaxErrorException("where statement num greater than 2.");
                }

                if (wherePhraseContexts.size() == 1) {
                    SqlParser.WherePhraseContext wherePhraseContext = wherePhraseContexts.get(0);
                    int whereStartIndex = wherePhraseContext.start.getStartIndex();
                    if (whereStartIndex < joinStartIndex) {
                        hasBeforeWhere = true;
                        hasAfterWhere = false;
                    } else {
                        hasBeforeWhere = false;
                        hasAfterWhere = true;
                    }
                }

            } else {
                wherePhraseNums = 0;
            }
        }

        if (havingPhraseNums == 1 && groupByPhraseNums != 1) {
            throw new SyntaxErrorException("having statement exist, but groupBy statement not exist.");
        }


    }

    private static void reset() {
        wherePhraseNums = 0;
        groupByPhraseNums = 0;
        havingPhraseNums = 0;
        hasBeforeWhere = false;
        hasAfterWhere = false;

        windowFunction = false;
        join = false;
    }
}
