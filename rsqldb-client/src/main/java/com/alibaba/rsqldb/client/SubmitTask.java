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

package com.alibaba.rsqldb.client;
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

import com.alibaba.rsqldb.client.http.HttpHelper;
import com.alibaba.rsqldb.client.util.FileUtil;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;

import static com.alibaba.rsqldb.client.constant.Constants.submitTask;

public class SubmitTask {

    public static void main(String[] args) throws Throwable {
        if (args == null || args.length < 1) {
            throw new IllegalArgumentException("home.dir is required.");
        }
        String homeDir = args[0];

        String sqlPath = homeDir + "/client/standalone.sql";

        File file = FileUtil.getFile(sqlPath);
        byte[] bytes = Files.readAllBytes(file.toPath());
        String sql = new String(bytes, StandardCharsets.UTF_8);

        HttpHelper.submitTask(submitTask, "test","test", sql);
    }
}
