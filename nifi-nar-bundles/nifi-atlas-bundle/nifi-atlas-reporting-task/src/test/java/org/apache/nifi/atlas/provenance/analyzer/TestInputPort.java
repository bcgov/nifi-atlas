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
package org.apache.nifi.atlas.provenance.analyzer;

import org.apache.atlas.typesystem.Referenceable;
import org.apache.nifi.atlas.provenance.AnalysisContext;
import org.apache.nifi.atlas.provenance.DataSetRefs;
import org.apache.nifi.atlas.provenance.NiFiProvenanceEventAnalyzer;
import org.apache.nifi.atlas.provenance.NiFiProvenanceEventAnalyzerFactory;
import org.apache.nifi.atlas.resolver.ClusterResolvers;
import org.apache.nifi.controller.status.ConnectionStatus;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.ProvenanceEventType;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.List;

import static org.apache.nifi.atlas.NiFiTypes.ATTR_NAME;
import static org.apache.nifi.atlas.NiFiTypes.ATTR_QUALIFIED_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.matches;
import static org.mockito.Mockito.when;

public class TestInputPort {

    @Test
    public void testInputPort() {
        final String processorName = "Input Port";

        ConnectionStatus con1 = Mockito.mock(ConnectionStatus.class);
        when(con1.getDestinationId()).thenReturn("101");

        List<ConnectionStatus> connectionStatuses = new ArrayList<>();
        connectionStatuses.add(con1);

        final String componentId = "100";
        final String transitUri = "http://52337acc0791:8080/nifi-api/data-transfer/input-ports/f335b506-015e-1000-0000-000022abd65c/transactions/8da2a04b-0e38-48de-b476-fe495b546d78/flow-files";
        final String sourceSystemFlowFileIdentifier = "urn:nifi:7ce27bc3-b128-4128-aba2-3a366435fd05";
        final ProvenanceEventRecord record = Mockito.mock(ProvenanceEventRecord.class);
        when(record.getComponentType()).thenReturn(processorName);
        when(record.getTransitUri()).thenReturn(transitUri);
        when(record.getComponentId()).thenReturn(componentId);
        when(record.getSourceSystemFlowFileIdentifier()).thenReturn(sourceSystemFlowFileIdentifier);
        when(record.getEventType()).thenReturn(ProvenanceEventType.RECEIVE);

        final ClusterResolvers clusterResolvers = Mockito.mock(ClusterResolvers.class);
        when(clusterResolvers.fromHostname(matches(".+\\.example\\.com"))).thenReturn("cluster1");

        final AnalysisContext context = Mockito.mock(AnalysisContext.class);
        when(context.getClusterResolver()).thenReturn(clusterResolvers);
        when(context.lookupInputPortName(componentId)).thenReturn("IN_PORT");
        when(context.findConnectionFrom(componentId)).thenReturn(connectionStatuses);

        final NiFiProvenanceEventAnalyzer analyzer = NiFiProvenanceEventAnalyzerFactory.getAnalyzer(processorName, transitUri, record.getEventType());
        assertNotNull(analyzer);

        final DataSetRefs refs = analyzer.analyze(context, record);
        assertEquals(1, refs.getComponentIds().size());
        assertEquals("101", refs.getComponentIds().iterator().next());
        assertEquals(1, refs.getInputs().size());
        assertEquals(0, refs.getOutputs().size());
        Referenceable ref = refs.getInputs().iterator().next();
        assertEquals("nifi_input_port", ref.getTypeName());
        assertEquals("IN_PORT", ref.get(ATTR_NAME));
        assertEquals("7ce27bc3-b128-4128-aba2-3a366435fd05", ref.get(ATTR_QUALIFIED_NAME));
    }

}
