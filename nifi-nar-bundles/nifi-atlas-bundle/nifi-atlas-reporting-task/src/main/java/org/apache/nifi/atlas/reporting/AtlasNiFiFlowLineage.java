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
package org.apache.nifi.atlas.reporting;

import org.apache.atlas.AtlasServiceException;
import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.atlas.typesystem.Referenceable;
import org.apache.atlas.typesystem.persistence.Id;
import org.apache.commons.lang.StringUtils;
import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.behavior.Stateful;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnUnscheduled;
import org.apache.nifi.atlas.*;
import org.apache.nifi.atlas.provenance.*;
import org.apache.nifi.atlas.resolver.ClusterResolver;
import org.apache.nifi.atlas.resolver.ClusterResolvers;
import org.apache.nifi.atlas.resolver.RegexClusterResolver;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.controller.status.ProcessorStatus;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.ProvenanceEventType;
import org.apache.nifi.provenance.ProvenanceRepository;
import org.apache.nifi.reporting.AbstractReportingTask;
import org.apache.nifi.reporting.EventAccess;
import org.apache.nifi.reporting.ReportingContext;
import org.apache.nifi.reporting.util.provenance.ProvenanceEventConsumer;
import org.apache.nifi.ssl.SSLContextService;
import org.apache.nifi.web.api.entity.ClusterEntity;

import javax.net.ssl.SSLContext;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.apache.commons.lang3.StringUtils.isEmpty;
import static org.apache.nifi.atlas.NiFiTypes.*;
import static org.apache.nifi.provenance.ProvenanceEventType.*;
import static org.apache.nifi.reporting.util.provenance.ProvenanceEventConsumer.PROVENANCE_BATCH_SIZE;
import static org.apache.nifi.reporting.util.provenance.ProvenanceEventConsumer.PROVENANCE_START_POSITION;

@Tags({"atlas", "lineage"})
@CapabilityDescription("Publishes NiFi flow data set level lineage to Apache Atlas." +
        " By reporting flow information to Atlas, an end-to-end Process and DataSet lineage such as across NiFi environments and other systems" +
        " connected by technologies, for example NiFi Site-to-Site, Kafka topic or Hive tables." +
        " There are limitations and required configurations for both NiFi and Atlas. See 'Additional Details' for further description.")
@Stateful(scopes = Scope.LOCAL, description = "Stores the Reporting Task's last event Id so that on restart the task knows where it left off.")
@DynamicProperty(name = "hostnamePattern.<ClusterName>", value = "hostname Regex patterns", description = RegexClusterResolver.PATTERN_PROPERTY_PREFIX_DESC)
public class AtlasNiFiFlowLineage extends AbstractReportingTask {

    static final PropertyDescriptor ATLAS_URLS = new PropertyDescriptor.Builder()
            .name("atlas-urls")
            .displayName("Atlas URLs")
            // TODO: Update doc to describe multiple URLs are for HA.
            .description("Comma separated URLs of the Atlas Server (e.g. http://atlas-server-hostname:21000).")
            .required(true)
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    static final PropertyDescriptor ATLAS_USER = new PropertyDescriptor.Builder()
            .name("atlas-username")
            .displayName("Atlas Username")
            .description("User name to communicate with Atlas.")
            .required(true)
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    static final PropertyDescriptor ATLAS_PASSWORD = new PropertyDescriptor.Builder()
            .name("atlas-password")
            .displayName("Atlas Password")
            .description("Password to communicate with Atlas.")
            .required(true)
            .sensitive(true)
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    static final PropertyDescriptor ATLAS_CONF_DIR = new PropertyDescriptor.Builder()
            .name("atlas-conf-dir")
            .displayName("Atlas Configuration Directory")
            .description("Directory path that contains 'atlas-application.properties' file." +
                    " If not specified, 'atlas-application.properties' file under root classpath is used.")
            .required(false)
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    static final PropertyDescriptor ATLAS_NIFI_URL = new PropertyDescriptor.Builder()
            .name("atlas-nifi-url")
            .displayName("NiFi URL for Atlas")
            .description("NiFi URL is used in Atlas to represent this NiFi cluster (or standalone instance)." +
                    " It is recommended to use one that can be accessible remotely instead of using 'localhost'.")
            .required(false)
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.URL_VALIDATOR)
            .build();

    public static final PropertyDescriptor NIFI_LINEAGE_STRATEGY = new PropertyDescriptor.Builder()
            .name("nifi-lineage-strategy")
            .displayName("NiFi Lineage Strategy")
            .description("Specifies what granularity the data should be sent to Atlas.  By Flow Path or by Unique File.")
            .required(true)
            .defaultValue("ByFlowPath")
            .expressionLanguageSupported(false)
            .allowableValues("ByFlowPath", "ByFlowFile")
            .build();

    static final PropertyDescriptor LOCAL_HOSTNAME = new PropertyDescriptor.Builder()
            .name("local-address")
            .displayName("Local Hostname")
            .description("This reporting task uses NiFi REST API against itself to retrieve NiFI flow data." +
                    " This hostname is used when making HTTP(s) requests to the REST API.")
            .required(true)
            .defaultValue("localhost")
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    static final PropertyDescriptor NIFI_API_PORT = new PropertyDescriptor.Builder()
            .name("nifi-api-port")
            .displayName("NiFi API Port")
            .description("Same as 'Local Hostname', this port number is used to specify a port number of this NiFi instance.")
            .required(true)
            .expressionLanguageSupported(true)
            .defaultValue("8080")
            .addValidator(StandardValidators.PORT_VALIDATOR)
            .build();

    public static final PropertyDescriptor NIFI_API_SECURE = new PropertyDescriptor.Builder()
            .name("nifi-api-secure")
            .displayName("NiFi API Secure")
            .description("Specify if this NiFi instance is secured and requires HTTPS. If true, 'SSL Context Service' needs to be set, too." +
                    " Also, NiFi security policy should be configured for this NiFi instance to read certain resources. See 'Additional Details' for further description.")
            .required(true)
            .expressionLanguageSupported(true)
            .allowableValues("true", "false")
            .defaultValue("false")
            .build();

    static final PropertyDescriptor SSL_CONTEXT = new PropertyDescriptor.Builder()
            .name("SSL Context Service")
            .description("The SSL Context Service to use when communicating with a secured NiFi node.")
            .required(false)
            .identifiesControllerService(SSLContextService.class)
            .build();

    private static final String ATLAS_PROPERTIES_FILENAME = "atlas-application.properties";
    private final ServiceLoader<ClusterResolver> clusterResolverLoader = ServiceLoader.load(ClusterResolver.class);
    private volatile NiFiAtlasClient atlasClient;
    private volatile Properties atlasProperties;
    private volatile boolean isTypeDefCreated = false;

    private volatile ProvenanceEventConsumer consumer;
    private volatile ClusterResolvers clusterResolvers;
    private volatile NiFIAtlasHook nifiAtlasHook;
    private volatile LineageStrategy lineageStrategy;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(ATLAS_URLS);
        properties.add(ATLAS_USER);
        properties.add(ATLAS_PASSWORD);
        properties.add(ATLAS_CONF_DIR);
        properties.add(ATLAS_NIFI_URL);
        properties.add(LOCAL_HOSTNAME);
        properties.add(NIFI_LINEAGE_STRATEGY);
        properties.add(NIFI_API_PORT);
        properties.add(NIFI_API_SECURE);
        properties.add(SSL_CONTEXT);
        properties.add(PROVENANCE_START_POSITION);
        properties.add(PROVENANCE_BATCH_SIZE);
        return properties;
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(String propertyDescriptorName) {
        for (ClusterResolver resolver : clusterResolverLoader) {
            final PropertyDescriptor propertyDescriptor = resolver.getSupportedDynamicPropertyDescriptor(propertyDescriptorName);
            if(propertyDescriptor != null) {
                return propertyDescriptor;
            }
        }
        return null;
    }

    private void parseAtlasUrls(final PropertyValue atlasUrlsProp, final Consumer<String> urlStrConsumer) {
        final String atlasUrlsStr = atlasUrlsProp.evaluateAttributeExpressions().getValue();
        if (atlasUrlsStr != null && !atlasUrlsStr.isEmpty()) {
            Arrays.stream(atlasUrlsStr.split(","))
                    .map(s -> s.trim())
                    .forEach(input -> urlStrConsumer.accept(input));
        }
    }

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
        final Collection<ValidationResult> results = new ArrayList<>();

        parseAtlasUrls(validationContext.getProperty(ATLAS_URLS), input -> {
            final ValidationResult.Builder builder = new ValidationResult.Builder().subject(ATLAS_URLS.getDisplayName()).input(input);
            try {
                new URL(input);
                results.add(builder.explanation("Valid URI").valid(true).build());
            } catch (Exception e) {
                results.add(builder.explanation("Contains invalid URI: " + e).valid(false).build());
            }
        });

        clusterResolverLoader.forEach(resolver -> results.addAll(resolver.validate(validationContext)));

        return results;
    }

    @OnScheduled
    public void setup(ConfigurationContext context) throws IOException {
        // initAtlasClient has to be done first as it loads AtlasProperty.
        initAtlasClient(context);
        initProvenanceConsumer(context);

        String strategy = context.getProperty(NIFI_LINEAGE_STRATEGY).getValue();
        lineageStrategy = "ByFlowFile".equals(strategy) ? LineageStrategy.BY_FILE : LineageStrategy.BY_PATH;
    }


    private void initAtlasClient(ConfigurationContext context) throws IOException {
        List<String> urls = new ArrayList<>();
        parseAtlasUrls(context.getProperty(ATLAS_URLS), url -> urls.add(url));

        final String user = context.getProperty(ATLAS_USER).getValue();
        final String password = context.getProperty(ATLAS_PASSWORD).getValue();
        final String confDirStr = context.getProperty(ATLAS_CONF_DIR).getValue();
        final File confDir = confDirStr != null && !confDirStr.isEmpty() ? new File(confDirStr) : null;

        atlasProperties = new Properties();
        final File atlasPropertiesFile = new File(confDir, ATLAS_PROPERTIES_FILENAME);
        if (atlasPropertiesFile.isFile()) {
            getLogger().info("Loading {}", new Object[]{confDir});
            try (InputStream in = new FileInputStream(atlasPropertiesFile)) {
                atlasProperties.load(in);
            }
        } else {
            final String fileInClasspath = "/" + ATLAS_PROPERTIES_FILENAME;
            try (InputStream in = AtlasNiFiFlowLineage.class.getResourceAsStream(fileInClasspath)) {
                getLogger().info("Loading {} from classpath", new Object[]{fileInClasspath});
                if (in == null) {
                    throw new ProcessException(String.format("Could not find %s from classpath.", fileInClasspath));
                }
                atlasProperties.load(in);
            }
        }

        atlasClient = NiFiAtlasClient.getInstance();
        try {
            atlasClient.initialize(true, urls.toArray(new String[]{}), user, password, confDir);
        } catch (final NullPointerException e) {
            throw new ProcessException(String.format("Failed to initialize Atlas client due to %s." +
                    " Make sure 'atlas-application.properties' is in the directory specified with %s" +
                    " or under root classpath if not specified.", e, ATLAS_CONF_DIR.getDisplayName()), e);
        }

    }

    private void initProvenanceConsumer(final ConfigurationContext context) throws IOException {
        consumer = new ProvenanceEventConsumer();
        consumer.setStartPositionValue(context.getProperty(PROVENANCE_START_POSITION).getValue());
        consumer.setBatchSize(context.getProperty(PROVENANCE_BATCH_SIZE).asInteger());
        consumer.addTargetEventType(CREATE, FETCH, RECEIVE, SEND, CLONE);
        consumer.setLogger(getLogger());
        consumer.setScheduled(true);

        final Set<ClusterResolver> loadedClusterResolvers = new LinkedHashSet<>();
        clusterResolverLoader.forEach(resolver -> {
            resolver.configure(context);
            loadedClusterResolvers.add(resolver);
        });
        clusterResolvers = new ClusterResolvers(Collections.unmodifiableSet(loadedClusterResolvers), null);

        nifiAtlasHook = new NiFIAtlasHook();
    }

    @OnUnscheduled
    public void onUnscheduled() {
        if (consumer != null) {
            consumer.setScheduled(false);
        }
    }

    @Override
    public void onTrigger(ReportingContext context) {

        final Boolean isNiFiApiSecure = context.getProperty(NIFI_API_SECURE).evaluateAttributeExpressions().asBoolean();
        final Integer nifiApiPort = context.getProperty(NIFI_API_PORT).evaluateAttributeExpressions().asInteger();
        final String localhost = context.getProperty(LOCAL_HOSTNAME).evaluateAttributeExpressions().getValue();
        final String nifiBaseUrl = (isNiFiApiSecure ? "https" : "http") + "://" + localhost + ":" + nifiApiPort + "/";
        final NiFiApiClient nifiClient = new NiFiApiClient(nifiBaseUrl);

        if (isNiFiApiSecure) {
            final SSLContextService sslContextService = context.getProperty(SSL_CONTEXT).asControllerService(SSLContextService.class);
            final SSLContext sslContext = sslContextService.createSSLContext(SSLContextService.ClientAuth.REQUIRED);
            nifiClient.setSslContext(sslContext);
        }

        final String clusterNodeId = context.getClusterNodeIdentifier();
        if (context.isClustered()) {
            if (isEmpty(clusterNodeId)) {
                // Clustered, but this node's ID is unknown. Not ready for processing yet.
                return;
            }
            /*
            Want to run on all nodes
            try {
                // /nifi-api/controller/cluster
                final ClusterEntity clusterEntity = nifiClient.getClusterEntity();
                LogUtils.log("Cluster Details", clusterEntity);
                if (clusterEntity.getCluster().getNodes().stream()
                        .noneMatch(node -> clusterNodeId.equals(node.getNodeId())
                                && node.getRoles().contains("Primary Node"))) {
                    // In a cluster, only primary node can report to Atlas.
                    // TODO: This should be done by NiFi scheduler like processor. But not supported at this moment.
                    return;
                }
            } catch (IOException e) {
                throw new RuntimeException("Failed to get cluster entity, due to " + e, e);
            }
            */
        }

        // Create Entity defs in Atlas if there's none yet.
        if (!isTypeDefCreated) {
            try {
                atlasClient.registerNiFiTypeDefs(false);
                isTypeDefCreated = true;
            } catch (AtlasServiceException e) {
                throw new RuntimeException("Failed to check and create NiFi flow type definitions in Atlas due to " + e, e);
            }
        }

        final NiFiFlowAnalyzer flowAnalyzer = new NiFiFlowAnalyzer();

        final NiFiFlow niFiFlow;
        try {
            final AtlasVariables atlasVariables = new AtlasVariables();
            atlasVariables.setNifiUrl(context.getProperty(ATLAS_NIFI_URL).evaluateAttributeExpressions().getValue());
            niFiFlow = flowAnalyzer.analyzeProcessGroup(atlasVariables, context);
        } catch (IOException e) {
            throw new RuntimeException("Failed to analyze NiFi flow. " + e, e);
        }

        try {
            flowAnalyzer.analyzePaths(niFiFlow);
            atlasClient.registerNiFiFlow(niFiFlow);
        } catch (AtlasServiceException e) {
            throw new RuntimeException("Failed to register NiFI flow. " + e, e);
        }

        consumeNiFiProvenanceEvents(context, niFiFlow);

    }

    private void consumeNiFiProvenanceEvents(ReportingContext context, NiFiFlow nifiFlow) {
        final EventAccess eventAccess = context.getEventAccess();
        final AnalysisContext analysisContext = new StandardAnalysisContext(nifiFlow, clusterResolvers,
                // FIXME: Class cast shouldn't be necessary to query lineage.
                (ProvenanceRepository)eventAccess.getProvenanceRepository());

        LineageEventProcessor processor = (lineageStrategy == LineageStrategy.BY_PATH ? new ByPathLineageStrategy(getLogger(), nifiAtlasHook) : new ByFileLineageStrategy(getLogger(), nifiAtlasHook) );

        consumer.consumeEvents(eventAccess, context.getStateManager(), events -> {
            for (ProvenanceEventRecord event : events) {

                LogUtils.log("ProvenanceEvent", event);

                processor.processEvent(event, nifiFlow, analysisContext);

            }
            nifiAtlasHook.commitMessages();
        });
    }
}
