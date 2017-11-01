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
package org.apache.nifi.atlas.provenance;

import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.atlas.notification.hook.HookNotification;
import org.apache.atlas.typesystem.Referenceable;
import org.apache.nifi.atlas.*;
import org.apache.nifi.atlas.resolver.ClusterResolvers;
import org.apache.nifi.controller.status.ProcessGroupStatus;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.ProvenanceEventType;
import org.apache.nifi.reporting.ReportingContext;
import org.apache.nifi.util.MockComponentLog;
import org.apache.nifi.util.MockEventAccess;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.apache.nifi.atlas.NiFiTypes.ATTR_NAME;
import static org.apache.nifi.atlas.NiFiTypes.ATTR_OUTPUTS;
import static org.apache.nifi.atlas.NiFiTypes.ATTR_QUALIFIED_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.matches;
import static org.mockito.Mockito.when;

public class TestByFileLineageStrategy {

    @Test
    public void testClone() throws IOException {

        ProcessGroupStatus root = new ProcessGroupStatus();

        MockComponentLog logger = new MockComponentLog("0", this);

        NiFIAtlasHook atlasHook = new NiFIAtlasHook() {
            protected void notifyEntities(List<HookNotification.HookNotificationMessage> messages) {

                assertEquals(4, messages.size());
                assertEquals(1, ((HookNotification.EntityCreateRequest)messages.get(0)).getEntities().size());
                assertEquals(1, ((HookNotification.EntityCreateRequest)messages.get(1)).getEntities().size());
                assertEquals(1, ((HookNotification.EntityCreateRequest)messages.get(2)).getEntities().size());
                assertEquals(1, ((HookNotification.EntityCreateRequest)messages.get(3)).getEntities().size());

                assertEquals("nifi_data", ((HookNotification.EntityCreateRequest)messages.get(0)).getEntities().get(0).getTypeName());
                assertEquals("nifi_flow_path", ((HookNotification.EntityCreateRequest)messages.get(1)).getEntities().get(0).getTypeName());
                assertEquals("nifi_data", ((HookNotification.EntityCreateRequest)messages.get(2)).getEntities().get(0).getTypeName());
                assertEquals("nifi_flow_path", ((HookNotification.EntityCreateRequest)messages.get(3)).getEntities().get(0).getTypeName());

                Referenceable nifiData0 = ((HookNotification.EntityCreateRequest)messages.get(0)).getEntities().get(0);
                assertEquals("child-guid-1", nifiData0.get(ATTR_QUALIFIED_NAME));

                Referenceable nifiData1 = ((HookNotification.EntityCreateRequest)messages.get(1)).getEntities().get(0);
                assertEquals("file-guid", nifiData1.get(ATTR_QUALIFIED_NAME));

                Referenceable nifiData2 = ((HookNotification.EntityCreateRequest)messages.get(2)).getEntities().get(0);
                assertEquals("child-guid-1", nifiData2.get(ATTR_QUALIFIED_NAME));

                Referenceable nifiData3 = ((HookNotification.EntityCreateRequest)messages.get(3)).getEntities().get(0);
                assertEquals("child-guid-1", nifiData3.get(ATTR_QUALIFIED_NAME));

            }
        };


        final ByFileLineageStrategy strategy = new ByFileLineageStrategy(logger, atlasHook);

        final ProvenanceEventRecord event = Mockito.mock(ProvenanceEventRecord.class);
        when(event.getComponentId()).thenReturn("comp-id");
        when(event.getComponentType()).thenReturn("InferAvroSchema");
        when(event.getFlowFileUuid()).thenReturn("file-guid");
        when(event.getChildUuids()).thenReturn(Arrays.asList("child-guid-1"));
        when(event.getAttribute("filename")).thenReturn("sample_z");
        when(event.getEventType()).thenReturn(ProvenanceEventType.CLONE);

        final ClusterResolvers clusterResolvers = Mockito.mock(ClusterResolvers.class);
        when(clusterResolvers.fromHostname(matches(".+\\.example\\.com"))).thenReturn("cluster1");

        final AnalysisContext context = Mockito.mock(AnalysisContext.class);
        when(context.getClusterResolver()).thenReturn(clusterResolvers);


        final NiFiFlowAnalyzer analyzer = new NiFiFlowAnalyzer();

        final AtlasVariables atlasVariables = new AtlasVariables();
        final MockEventAccess eventAccess = Mockito.mock(MockEventAccess.class);
        when (eventAccess.getGroupStatus("root")).thenReturn(root);

        ReportingContext reportingContext = Mockito.mock(ReportingContext.class);
        when(reportingContext.getEventAccess()).thenReturn(eventAccess);

        final NiFiFlow nifiFlow = analyzer.analyzeProcessGroup(atlasVariables, reportingContext);

        strategy.processEvent(event, nifiFlow, context);

        atlasHook.commitMessages();
    }

}
