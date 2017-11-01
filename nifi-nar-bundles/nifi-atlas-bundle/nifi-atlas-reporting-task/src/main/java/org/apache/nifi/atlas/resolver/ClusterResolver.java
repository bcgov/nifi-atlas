package org.apache.nifi.atlas.resolver;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.context.PropertyContext;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;

public interface ClusterResolver {

    default Collection<ValidationResult> validate(final ValidationContext validationContext) {
        return Collections.emptySet();
    }

    PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName);

    /**
     * Implementation should clear previous configurations when this method is called again.
     * @param context passed from ReportingTask
     */
    void configure(PropertyContext context);

    /**
     * Resolve a cluster name from a hostname or an ip address.
     * @param hostname hostname or ip address
     * @return resolved cluster name or null
     */
    default String fromHostname(String hostname) {
        return null;
    }

    /**
     * Resolve a cluster name from hints, such as Zookeeper Quorum, client port and znode path
     * @param hints Contains variables to resolve a cluster name
     * @return resolved cluster name or null
     */
    default String fromHints(Map<String, String> hints) {
        return null;
    }

}
