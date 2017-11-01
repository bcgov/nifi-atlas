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

import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.nifi.controller.status.ConnectionStatus;
import org.apache.nifi.controller.status.PortStatus;
import org.apache.nifi.controller.status.ProcessGroupStatus;
import org.apache.nifi.controller.status.ProcessorStatus;
import org.apache.nifi.reporting.EventAccess;
import org.apache.nifi.reporting.ReportingContext;
import org.apache.nifi.util.Tuple;
import org.apache.nifi.web.api.dto.ProcessGroupDTO;
import org.apache.nifi.web.api.entity.ProcessGroupEntity;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.nifi.atlas.NiFiTypes.ATTR_QUALIFIED_NAME;
import static org.apache.nifi.atlas.NiFiTypes.TYPE_NIFI_QUEUE;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.matches;
import static org.mockito.Mockito.when;

public class TestNiFiFlowAnalyzer {

    private int componentId = 0;
    private AtlasVariables atlasVariables;

    @Before
    public void before() throws Exception {
        componentId = 0;
        atlasVariables = new AtlasVariables();
    }

    private ProcessGroupStatus createEmptyProcessGroupStatus() {
        final ProcessGroupStatus processGroupStatus = new ProcessGroupStatus();

        processGroupStatus.setId(nextComponentId());
        processGroupStatus.setName("Flow name");

        return processGroupStatus;
    }

    private ProcessGroupEntity createProcessGroupEntity() {
        final ProcessGroupEntity entity = new ProcessGroupEntity();
        ProcessGroupDTO processGroup = new ProcessGroupDTO();
        processGroup.setComments("Flow comment");
        entity.setComponent(processGroup);
        return entity;
    }

    @Test
    public void testEmptyFlow() throws Exception {

        ReportingContext reportingContext = Mockito.mock(ReportingContext.class);
        EventAccess eventAccess = Mockito.mock(EventAccess.class);

        ProcessGroupStatus rootPG = createEmptyProcessGroupStatus();

        when(reportingContext.getEventAccess()).thenReturn(eventAccess);
        when(eventAccess.getGroupStatus(matches("root"))).thenReturn(rootPG);

        final NiFiFlowAnalyzer analyzer = new NiFiFlowAnalyzer();

        final NiFiFlow nifiFlow = analyzer.analyzeProcessGroup(atlasVariables, reportingContext);

        assertEquals("Flow name", nifiFlow.getFlowName());
//        assertEquals("Flow comment", nifiFlow.getDescription());
    }

    private ProcessorStatus createProcessor(ProcessGroupStatus pgStatus, String type) {
        final ProcessorStatus processor = new ProcessorStatus();
        processor.setName(type);
        processor.setId(nextComponentId());
        pgStatus.getProcessorStatus().add(processor);

        return  processor;
    }

    private String nextComponentId() {
        return String.format("1234-5678-0000-%04d", componentId++);
    }

    private void connect(ProcessGroupStatus pg0, Object o0, Object o1) {
        Function<Object, Tuple<String, String>> toTupple = o -> {
            Tuple<String, String> comp;
            if (o instanceof ProcessorStatus) {
                ProcessorStatus p = (ProcessorStatus) o;
                comp = new Tuple<>(p.getId(), p.getName());
            } else if (o instanceof PortStatus) {
                PortStatus p = (PortStatus) o;
                comp = new Tuple<>(p.getId(), p.getName());
            } else {
                throw new IllegalArgumentException("Not supported");
            }
            return comp;
        };
        connect(pg0, toTupple.apply(o0), toTupple.apply(o1));
    }

    private void connect(ProcessGroupStatus pg0, Tuple<String, String> comp0, Tuple<String, String> comp1) {
        ConnectionStatus conn = new ConnectionStatus();
        conn.setId(nextComponentId());
        conn.setGroupId(pg0.getId());

        conn.setSourceId(comp0.getKey());
        conn.setSourceName(comp0.getValue());

        conn.setDestinationId(comp1.getKey());
        conn.setDestinationName(comp1.getValue());

        pg0.getConnectionStatus().add(conn);
    }

    @Test
    public void testSingleProcessor() throws Exception {

        ReportingContext reportingContext = Mockito.mock(ReportingContext.class);
        EventAccess eventAccess = Mockito.mock(EventAccess.class);

        ProcessGroupStatus rootPG = createEmptyProcessGroupStatus();

        when(reportingContext.getEventAccess()).thenReturn(eventAccess);
        when(eventAccess.getGroupStatus(matches("root"))).thenReturn(rootPG);

        final ProcessorStatus pr0 = createProcessor(rootPG, "GenerateFlowFile");


        final NiFiFlowAnalyzer analyzer = new NiFiFlowAnalyzer();

        final NiFiFlow nifiFlow = analyzer.analyzeProcessGroup(atlasVariables, reportingContext);

        assertEquals(1, nifiFlow.getProcessors().size());

        analyzer.analyzePaths(nifiFlow);
        final List<NiFiFlowPath> paths = nifiFlow.getFlowPaths();

        assertEquals(1, paths.size());

        final NiFiFlowPath path0 = paths.get(0);
        assertEquals(path0.getId(), path0.getProcessorIds().get(0));

        // Should be able to find a path from a given processor GUID.
        final NiFiFlowPath pathForPr0 = nifiFlow.findPath(pr0.getId());
        assertEquals(path0, pathForPr0);
    }


    @Test
    public void testProcessorsWithinSinglePath() throws Exception {

        ReportingContext reportingContext = Mockito.mock(ReportingContext.class);
        EventAccess eventAccess = Mockito.mock(EventAccess.class);

        ProcessGroupStatus rootPG = createEmptyProcessGroupStatus();

        when(reportingContext.getEventAccess()).thenReturn(eventAccess);
        when(eventAccess.getGroupStatus(matches("root"))).thenReturn(rootPG);

        final ProcessorStatus pr0 = createProcessor(rootPG, "GenerateFlowFile");
        final ProcessorStatus pr1 = createProcessor(rootPG, "UpdateAttribute");

        connect(rootPG, pr0, pr1);

        final NiFiFlowAnalyzer analyzer = new NiFiFlowAnalyzer();

        final NiFiFlow nifiFlow = analyzer.analyzeProcessGroup(atlasVariables, reportingContext);

        assertEquals(2, nifiFlow.getProcessors().size());

        analyzer.analyzePaths(nifiFlow);
        final List<NiFiFlowPath> paths = nifiFlow.getFlowPaths();

        assertEquals(1, paths.size());

        // Should be able to find a path from a given processor GUID.
        final NiFiFlowPath pathForPr0 = nifiFlow.findPath(pr0.getId());
        final NiFiFlowPath pathForPr1 = nifiFlow.findPath(pr1.getId());
        final NiFiFlowPath path0 = paths.get(0);
        assertEquals(path0, pathForPr0);
        assertEquals(path0, pathForPr1);
    }

    @Test
    public void testMultiPaths() throws Exception {

        ReportingContext reportingContext = Mockito.mock(ReportingContext.class);
        EventAccess eventAccess = Mockito.mock(EventAccess.class);

        ProcessGroupStatus rootPG = createEmptyProcessGroupStatus();

        when(reportingContext.getEventAccess()).thenReturn(eventAccess);
        when(eventAccess.getGroupStatus(matches("root"))).thenReturn(rootPG);


        final ProcessorStatus pr0 = createProcessor(rootPG, "GenerateFlowFile");
        final ProcessorStatus pr1 = createProcessor(rootPG, "UpdateAttribute");
        final ProcessorStatus pr2 = createProcessor(rootPG, "ListenTCP");
        final ProcessorStatus pr3 = createProcessor(rootPG, "LogAttribute");

        connect(rootPG, pr0, pr1);
        connect(rootPG, pr2, pr3);

        final NiFiFlowAnalyzer analyzer = new NiFiFlowAnalyzer();

        final NiFiFlow nifiFlow = analyzer.analyzeProcessGroup(atlasVariables, reportingContext);

        assertEquals(4, nifiFlow.getProcessors().size());

        analyzer.analyzePaths(nifiFlow);
        final List<NiFiFlowPath> paths = nifiFlow.getFlowPaths();

        assertEquals(2, paths.size());

        // Order is not guaranteed
        final Map<String, NiFiFlowPath> pathMap = paths.stream().collect(Collectors.toMap(p -> p.getId(), p -> p));
        final NiFiFlowPath pathA = pathMap.get(pr0.getId());
        final NiFiFlowPath pathB = pathMap.get(pr2.getId());
        assertEquals(2, pathA.getProcessorIds().size());
        assertEquals(2, pathB.getProcessorIds().size());

        // Should be able to find a path from a given processor GUID.
        final NiFiFlowPath pathForPr0 = nifiFlow.findPath(pr0.getId());
        final NiFiFlowPath pathForPr1 = nifiFlow.findPath(pr1.getId());
        final NiFiFlowPath pathForPr2 = nifiFlow.findPath(pr2.getId());
        final NiFiFlowPath pathForPr3 = nifiFlow.findPath(pr3.getId());
        assertEquals(pathA, pathForPr0);
        assertEquals(pathA, pathForPr1);
        assertEquals(pathB, pathForPr2);
        assertEquals(pathB, pathForPr3);
    }

    @Test
    public void testMultiPathsJoint() throws Exception {

        ReportingContext reportingContext = Mockito.mock(ReportingContext.class);
        EventAccess eventAccess = Mockito.mock(EventAccess.class);

        ProcessGroupStatus rootPG = createEmptyProcessGroupStatus();

        when(reportingContext.getEventAccess()).thenReturn(eventAccess);
        when(eventAccess.getGroupStatus(matches("root"))).thenReturn(rootPG);

        final ProcessorStatus pr0 = createProcessor(rootPG, "org.apache.nifi.processors.standard.GenerateFlowFile");
        final ProcessorStatus pr1 = createProcessor(rootPG, "org.apache.nifi.processors.standard.UpdateAttribute");
        final ProcessorStatus pr2 = createProcessor(rootPG, "org.apache.nifi.processors.standard.ListenTCP");
        final ProcessorStatus pr3 = createProcessor(rootPG, "org.apache.nifi.processors.standard.LogAttribute");

        // Result should be as follows:
        // pathA = 0 -> 1 (-> 3)
        // pathB = 2 (-> 3)
        // pathC = 3
        connect(rootPG, pr0, pr1);
        connect(rootPG, pr1, pr3);
        connect(rootPG, pr2, pr3);

        final NiFiFlowAnalyzer analyzer = new NiFiFlowAnalyzer();

        final NiFiFlow nifiFlow = analyzer.analyzeProcessGroup(atlasVariables, reportingContext);

        assertEquals(4, nifiFlow.getProcessors().size());

        analyzer.analyzePaths(nifiFlow);
        final List<NiFiFlowPath> paths = nifiFlow.getFlowPaths();

        assertEquals(3, paths.size());

        // Order is not guaranteed
        final Map<String, NiFiFlowPath> pathMap = paths.stream().collect(Collectors.toMap(p -> p.getId(), p -> p));
        final NiFiFlowPath pathA = pathMap.get(pr0.getId());
        final NiFiFlowPath pathB = pathMap.get(pr2.getId());
        final NiFiFlowPath pathC = pathMap.get(pr3.getId());
        assertEquals(2, pathA.getProcessorIds().size());
        assertEquals(1, pathB.getProcessorIds().size());
        assertEquals(1, pathC.getProcessorIds().size());

        // A queue is added as input for the joint point.
        assertEquals(1, pathC.getInputs().size());
        final AtlasObjectId queue = pathC.getInputs().iterator().next();
        assertEquals(TYPE_NIFI_QUEUE, queue.getTypeName());
        assertEquals(pathC.getId(), queue.getUniqueAttributes().get(ATTR_QUALIFIED_NAME));

        // Should be able to find a path from a given processor GUID.
        final NiFiFlowPath pathForPr0 = nifiFlow.findPath(pr0.getId());
        final NiFiFlowPath pathForPr1 = nifiFlow.findPath(pr1.getId());
        final NiFiFlowPath pathForPr2 = nifiFlow.findPath(pr2.getId());
        final NiFiFlowPath pathForPr3 = nifiFlow.findPath(pr3.getId());
        assertEquals(pathA, pathForPr0);
        assertEquals(pathA, pathForPr1);
        assertEquals(pathB, pathForPr2);
        assertEquals(pathC, pathForPr3);
    }

    @Test
    public void testRootGroupPorts() throws Exception {

        ReportingContext reportingContext = Mockito.mock(ReportingContext.class);
        EventAccess eventAccess = Mockito.mock(EventAccess.class);

        ProcessGroupStatus rootPG = createEmptyProcessGroupStatus();

        when(reportingContext.getEventAccess()).thenReturn(eventAccess);
        when(eventAccess.getGroupStatus(matches("root"))).thenReturn(rootPG);

        final ProcessorStatus pr0 = createProcessor(rootPG, "org.apache.nifi.processors.standard.GenerateFlowFile");
        final ProcessorStatus pr1 = createProcessor(rootPG, "org.apache.nifi.processors.standard.UpdateAttribute");
        final ProcessorStatus pr2 = createProcessor(rootPG, "org.apache.nifi.processors.standard.LogAttribute");

        PortStatus inputPort1 = createInputPortStatus(rootPG, "input-1");
        PortStatus outputPort1 = createOutputPortStatus(rootPG, "output-1");

        connect(rootPG, pr0, outputPort1);
        connect(rootPG, inputPort1, pr1);
        connect(rootPG, pr1, pr2);

        final NiFiFlowAnalyzer analyzer = new NiFiFlowAnalyzer();

        final NiFiFlow nifiFlow = analyzer.analyzeProcessGroup(atlasVariables, reportingContext);

        assertEquals(3, nifiFlow.getProcessors().size());

        analyzer.analyzePaths(nifiFlow);
        final List<NiFiFlowPath> paths = nifiFlow.getFlowPaths();

        // pathA: GenerateFlowFile(pr0) -> output-1
        // pathB: input-1 -> UpdateAttribute(pr1) -> LogAttribute
        assertEquals(2, paths.size());
        final Map<String, NiFiFlowPath> pathMap = paths.stream().collect(Collectors.toMap(p -> p.getId(), p -> p));
        final NiFiFlowPath pathA = pathMap.get(pr0.getId());
        final NiFiFlowPath pathB = pathMap.get(pr1.getId());

        // TODO: This part should be created via lineage.
        assertEquals(1, pathA.getInputs().size()); // Obscure Ingress
        // TODO: is this a remote output port?
        assertEquals(1, pathA.getOutputs().size());
        final AtlasObjectId output1 = pathA.getOutputs().iterator().next();
        assertEquals("nifi_output_port", output1.getTypeName());
        assertEquals(outputPort1.getId(), output1.getUniqueAttributes().get(ATTR_QUALIFIED_NAME));

        assertEquals(1, pathB.getInputs().size());
        assertEquals(0, pathB.getOutputs().size());
        final AtlasObjectId input1 = pathB.getInputs().iterator().next();
        assertEquals("nifi_input_port", input1.getTypeName());
        assertEquals(inputPort1.getId(), input1.getUniqueAttributes().get(ATTR_QUALIFIED_NAME));

    }

    private PortStatus createOutputPortStatus(ProcessGroupStatus pg, String name) {
        return createPortStatus(pg, name, false);
    }

    private PortStatus createInputPortStatus(ProcessGroupStatus pg, String name) {
        return createPortStatus(pg, name, true);
    }

    private PortStatus createPortStatus(ProcessGroupStatus pg, String name, boolean isInputPort) {
        final PortStatus port = new PortStatus();
        if (isInputPort) {
            pg.getInputPortStatus().add(port);
        } else {
            pg.getOutputPortStatus().add(port);
        }

        port.setId(nextComponentId());
        port.setName(name);

        return port;
    }

    @Test
    public void testRootGroupPortsAndChildProcessGroup() throws Exception {

        ReportingContext reportingContext = Mockito.mock(ReportingContext.class);
        EventAccess eventAccess = Mockito.mock(EventAccess.class);

        ProcessGroupStatus rootPG = createEmptyProcessGroupStatus();
        ProcessGroupStatus childPG1 = createEmptyProcessGroupStatus();
        ProcessGroupStatus childPG2 = createEmptyProcessGroupStatus();

        when(reportingContext.getEventAccess()).thenReturn(eventAccess);
        when(eventAccess.getGroupStatus(matches("root"))).thenReturn(rootPG);

        final Collection<ProcessGroupStatus> childPGs = rootPG.getProcessGroupStatus();
        childPGs.add(childPG1);
        childPGs.add(childPG2);


        final ProcessorStatus pr0 = createProcessor(childPG1, "GenerateFlowFile");
        final ProcessorStatus pr1 = createProcessor(childPG2, "UpdateAttribute");
        final ProcessorStatus pr2 = createProcessor(childPG2, "LogAttribute");

        PortStatus inputPort1 = createInputPortStatus(rootPG, "input-1");
        PortStatus outputPort1 = createOutputPortStatus(rootPG, "output-1");

        final PortStatus childOutput = createOutputPortStatus(childPG1, "child-output");
        final PortStatus childInput = createOutputPortStatus(childPG2, "child-input");

        // TODO: check if this is correct.
        // From GenerateFlowFile in a child pg to a root group input port.
        connect(childPG1, pr0, childOutput);
        connect(childPG1, childOutput, inputPort1);

        // From a root group input port to an input port within a child port then connects to processor.
        connect(rootPG, inputPort1, childInput);
        connect(childPG2, childInput, pr1);
        connect(childPG2, pr1, pr2);

        final NiFiFlowAnalyzer analyzer = new NiFiFlowAnalyzer();

        final NiFiFlow nifiFlow = analyzer.analyzeProcessGroup(atlasVariables, reportingContext);
        nifiFlow.dump();

        assertEquals(3, nifiFlow.getProcessors().size());

        analyzer.analyzePaths(nifiFlow);
        final List<NiFiFlowPath> paths = nifiFlow.getFlowPaths();

        // input-1 -> child-input
        // pathA: GenerateFlowFile(pr0) -> child-output-1 -> input-1
        // pathB: child-input -> UpdateAttribute(pr1) -> LogAttribute(pr2)
        assertEquals(2, paths.size());
        final Map<String, NiFiFlowPath> pathMap = paths.stream().collect(Collectors.toMap(p -> p.getId(), p -> p));
        final NiFiFlowPath pathA = pathMap.get(pr0.getId());
        final NiFiFlowPath pathB = pathMap.get(pr1.getId());

        // TODO: This part should be done by provenance.
        assertEquals(1, pathA.getInputs().size()); // Obscure Ingress
        assertEquals(1, pathA.getOutputs().size());
        final AtlasObjectId output1 = pathA.getOutputs().iterator().next();
        assertEquals("nifi_output_port", output1.getTypeName());
        assertEquals(outputPort1.getId(), output1.getUniqueAttributes().get(ATTR_QUALIFIED_NAME));

        assertEquals(1, pathB.getInputs().size());
        assertEquals(0, pathB.getOutputs().size());
        final AtlasObjectId input1 = pathB.getInputs().iterator().next();
        assertEquals("nifi_input_port", input1.getTypeName());
        assertEquals(inputPort1.getId(), input1.getUniqueAttributes().get(ATTR_QUALIFIED_NAME));

    }

}
