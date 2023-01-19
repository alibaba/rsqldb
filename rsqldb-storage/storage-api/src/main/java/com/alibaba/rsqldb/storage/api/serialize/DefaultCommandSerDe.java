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
package com.alibaba.rsqldb.storage.api.serialize;

import com.alibaba.rsqldb.common.SerializeType;
import com.alibaba.rsqldb.common.exception.DeserializeException;
import com.alibaba.rsqldb.common.exception.SerializeException;
import com.alibaba.rsqldb.parser.model.Node;
import com.alibaba.rsqldb.parser.model.statement.Statement;
import com.alibaba.rsqldb.parser.serialization.Deserializer;
import com.alibaba.rsqldb.parser.serialization.SerializeTypeContainer;
import com.alibaba.rsqldb.parser.serialization.Serializer;
import com.alibaba.rsqldb.storage.api.Command;
import com.alibaba.rsqldb.storage.api.CommandSerDe;
import com.alibaba.rsqldb.storage.api.CommandStatus;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.ConcurrentHashMap;

public class DefaultCommandSerDe implements CommandSerDe {
    private static final Logger logger = LoggerFactory.getLogger(DefaultCommandSerDe.class);

    private static final ByteBuf buf = Unpooled.buffer(16);
    private static final Serializer serializer = SerializeTypeContainer.getSerializer(SerializeType.JSON);
    private static final Deserializer deserializer = SerializeTypeContainer.getDeserializer(SerializeType.JSON);
    private static final ConcurrentHashMap<String, Class<Node>> cache = new ConcurrentHashMap<>();

    @Override
    public byte[] serialize(Command command) throws SerializeException {
        if (command == null) {
            return new byte[0];
        }

        String jobId = command.getJobId();
        if (StringUtils.isBlank(jobId)) {
            logger.error("jobId is blank.");
            throw new SerializeException("jobId is blank.");
        }
        Node node = command.getNode();
        CommandStatus status = command.getStatus();
        if (status == null) {
            logger.error("status is blank.");
            throw new SerializeException("status is blank.");
        }

        //ser
        byte[] jobIdBytes = jobId.getBytes(StandardCharsets.UTF_8);

        byte[] nodeBytes = new byte[0];
        byte[] nodeClassBytes = new byte[0];
        if (node instanceof Statement) {
            Statement statement = (Statement) node;
            nodeBytes = serializer.serialize(statement);

            nodeClassBytes = node.getClass().getName().getBytes(StandardCharsets.UTF_8);
        } else if (node == null) {
            nodeBytes = new byte[0];
            nodeClassBytes = new byte[0];
        } else {
            throw new UnsupportedOperationException("please support it first.");
        }

        byte[] statusBytes = status.name().getBytes(StandardCharsets.UTF_8);

        //jobId
        buf.writeInt(jobIdBytes.length);
        buf.writeBytes(jobIdBytes);

        //node
        //1.class
        buf.writeInt(nodeClassBytes.length);
        if (nodeClassBytes.length != 0) {
            buf.writeBytes(nodeClassBytes);
        }

        //2.
        buf.writeInt(nodeBytes.length);
        if (nodeBytes.length != 0) {
            buf.writeBytes(nodeBytes);
        }

        //status
        buf.writeInt(statusBytes.length);
        buf.writeBytes(statusBytes);

        byte[] bytes = new byte[buf.readableBytes()];
        buf.readBytes(bytes);

        buf.clear();
        return bytes;
    }

    @SuppressWarnings("unchecked")
    @Override
    public Command deserialize(byte[] source) throws DeserializeException {
        ByteBuf byteBuf = Unpooled.wrappedBuffer(source);

        //jobId
        int jobIdLength = byteBuf.readInt();
        ByteBuf buf = byteBuf.readBytes(jobIdLength);
        byte[] jobIdBytes = new byte[jobIdLength];
        buf.readBytes(jobIdBytes);

        String jobId = new String(jobIdBytes, StandardCharsets.UTF_8);

        //node
        int nodeClassLength = byteBuf.readInt();
        String nodeClassName = null;
        if (nodeClassLength != 0) {
            ByteBuf nodeClassBuf = byteBuf.readBytes(nodeClassLength);
            byte[] nodeClassBytes = new byte[nodeClassLength];
            nodeClassBuf.readBytes(nodeClassBytes);

            nodeClassName = new String(nodeClassBytes, StandardCharsets.UTF_8);
            cache.computeIfAbsent(nodeClassName, name -> {
                try {
                    return (Class<Node>) Class.forName(name);
                } catch (ClassNotFoundException e) {
                    logger.error("can not find this class, class name:{}", name);
                    throw new RuntimeException(e);
                }
            });
            nodeClassBuf.release();
        }

        Node node = null;
        int nodeLength = byteBuf.readInt();
        if (nodeLength != 0) {
            ByteBuf nodeBuf = byteBuf.readBytes(nodeLength);
            byte[] nodeBytes = new byte[nodeLength];
            nodeBuf.readBytes(nodeBytes);
            node = deserializer.deserialize(nodeBytes, cache.get(nodeClassName));

            nodeBuf.release();
        }

        //status
        int statusLength = byteBuf.readInt();
        ByteBuf statusBuf = byteBuf.readBytes(statusLength);
        byte[] statusBytes = new byte[statusLength];
        statusBuf.readBytes(statusBytes);

        String status = new String(statusBytes, StandardCharsets.UTF_8);
        CommandStatus commandStatus = CommandStatus.valueOf(status);

        byteBuf.release();
        buf.release();
        statusBuf.release();

        return new Command(jobId, node, commandStatus);
    }
}
