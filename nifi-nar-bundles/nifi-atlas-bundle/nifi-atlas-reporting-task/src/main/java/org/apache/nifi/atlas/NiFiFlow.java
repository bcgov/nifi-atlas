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
package org.apache.nifi.atlas;

import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.nifi.controller.status.ConnectionStatus;
import org.apache.nifi.controller.status.PortStatus;
import org.apache.nifi.controller.status.ProcessorStatus;
import org.apache.nifi.controller.status.RemoteProcessGroupStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import static org.apache.nifi.atlas.NiFiTypes.ATTR_QUALIFIED_NAME;
import static org.apache.nifi.atlas.NiFiTypes.TYPE_NIFI_FLOW;

public class NiFiFlow {

    private static final Logger logger = LoggerFactory.getLogger(NiFiFlow.class);

    private final String flowName;
    private final String rootProcessGroupId;
    private final String url;
    private final AtlasObjectId id;
    private String description;
    private final Set<AtlasObjectId> inputs = new HashSet<>();
    private final Set<AtlasObjectId> outputs = new HashSet<>();
    private final List<NiFiFlowPath> flowPaths = new ArrayList<>();
    private final Map<String, Set<AtlasObjectId>> processorInputs = new HashMap<>();
    private final Map<String, Set<AtlasObjectId>> processorOutputs = new HashMap<>();
    private final Map<String, ProcessorStatus> processors = new HashMap<>();
    private final Map<String, RemoteProcessGroupStatus> remoteProcessGroups = new HashMap<>();
    private final Map<String, List<ConnectionStatus>> incomingRelationShips = new HashMap<>();
    private final Map<String, List<ConnectionStatus>> outGoingRelationShips = new HashMap<>();

    private final Map<AtlasObjectId, AtlasEntity> createdData = new HashMap<>();
    private final Map<AtlasObjectId, AtlasEntity> queues = new HashMap<>();
    // Any Ports.
    private final Map<String, PortStatus> inputPorts = new HashMap<>();
    private final Map<String, PortStatus> outputPorts = new HashMap<>();
    // Root Group Ports.
    private final Map<String, PortStatus> rootInputPorts = new HashMap<>();
    private final Map<String, PortStatus> rootOutputPorts = new HashMap<>();
    // Root Group Ports Entity.
    private final Map<AtlasObjectId, AtlasEntity> rootInputPortEntities = new HashMap<>();
    private final Map<AtlasObjectId, AtlasEntity> rootOutputPortEntities = new HashMap<>();

    public NiFiFlow(String flowName, String rootProcessGroupId, String url) {
        this.flowName = flowName;
        id = new AtlasObjectId(TYPE_NIFI_FLOW, ATTR_QUALIFIED_NAME, rootProcessGroupId);
        this.rootProcessGroupId = rootProcessGroupId;
        this.url = url;
    }

    public AtlasObjectId getId() {
        return id;
    }

    public String getRootProcessGroupId() {
        return rootProcessGroupId;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public void addConnection(ConnectionStatus c) {
        outGoingRelationShips.computeIfAbsent(c.getSourceId(), k -> new ArrayList<>()).add(c);
        incomingRelationShips.computeIfAbsent(c.getDestinationId(), k -> new ArrayList<>()).add(c);
    }

    public void addProcessor(ProcessorStatus p) {
        processors.put(p.getId(), p);
    }

    public Map<String, ProcessorStatus> getProcessors() {
        return processors;
    }

    public void addRemoteProcessGroup(RemoteProcessGroupStatus r) {
        remoteProcessGroups.put(r.getId(), r);
    }

    public String getFlowName() {
        return flowName;
    }

    public String getUrl() {
        return url;
    }

    public List<ConnectionStatus> getIncomingRelationShips(String componentId) {
        return incomingRelationShips.get(componentId);
    }

    public List<ConnectionStatus> getOutgoingRelationShips(String componentId) {
        return outGoingRelationShips.get(componentId);
    }

    public Set<AtlasObjectId> getInputs(String processorId) {
        return processorInputs.get(processorId);
    }

    public Set<AtlasObjectId> getOutputs(String processorId) {
        return processorOutputs.get(processorId);
    }

    public void addInputPort(PortStatus port) {
        inputPorts.put(port.getId(), port);
    }

    public Map<String, PortStatus> getInputPorts() {
        return inputPorts;
    }

    public void addOutputPort(PortStatus port) {
        outputPorts.put(port.getId(), port);
    }

    public Map<String, PortStatus> getOutputPorts() {
        return outputPorts;
    }

    public void addRootInputPort(PortStatus port) {
        rootInputPorts.put(port.getId(), port);
    }

    public Map<String, PortStatus> getRootInputPorts() {
        return rootInputPorts;
    }

    public void addRootOutputPort(PortStatus port) {
        rootOutputPorts.put(port.getId(), port);
    }

    public Map<String, PortStatus> getRootOutputPorts() {
        return rootOutputPorts;
    }

    public Map<AtlasObjectId, AtlasEntity> getRootInputPortEntities() {
        return rootInputPortEntities;
    }

    public Map<AtlasObjectId, AtlasEntity> getRootOutputPortEntities() {
        return rootOutputPortEntities;
    }

    public Map<AtlasObjectId, AtlasEntity> getQueues() {
        return queues;
    }

    public Map<AtlasObjectId, AtlasEntity> getCreatedData() {
        return createdData;
    }

    public List<NiFiFlowPath> getFlowPaths() {
        return flowPaths;
    }

    public NiFiFlowPath findPath(String processorGuid) {
        for (NiFiFlowPath path: flowPaths) {
            if (path.getProcessorIds().contains(processorGuid)){
                return path;
            }
        }
        return null;
    }

    public boolean isProcessor(String componentId) {
        return processors.containsKey(componentId);
    }

    public boolean isInputPort(String componentId) {
        return inputPorts.containsKey(componentId);
    }

    public boolean isOutputPort(String componentId) {
        return outputPorts.containsKey(componentId);
    }

    public boolean isRootInputPort(String componentId) {
        return rootInputPorts.containsKey(componentId);
    }

    public boolean isRootOutputPort(String componentId) {
        return rootOutputPorts.containsKey(componentId);
    }

    public void dump() {
        logger.info("flowName: {}", flowName);
        Function<String, String> toName = pid -> processors.get(pid).getName();
        processors.forEach((pid, p) -> {
            logger.info("{}:{} receives from {}", pid, toName.apply(pid), incomingRelationShips.get(pid));
            logger.info("{}:{} sends to {}", pid, toName.apply(pid), outGoingRelationShips.get(pid));
        });

        logger.info("## Input ObjectIds");
        inputs.forEach(in -> logger.info("{}", in));
        logger.info("## Output ObjectIds");
        outputs.forEach(out -> logger.info("{}", out));
    }

}
