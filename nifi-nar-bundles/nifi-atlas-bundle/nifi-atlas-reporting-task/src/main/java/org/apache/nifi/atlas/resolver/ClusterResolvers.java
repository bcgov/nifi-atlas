package org.apache.nifi.atlas.resolver;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.context.PropertyContext;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

public class ClusterResolvers implements ClusterResolver {
    private final Set<ClusterResolver> resolvers;

    private final String defaultClusterName;

    public ClusterResolvers(Set<ClusterResolver> resolvers, String defaultClusterName) {
        this.resolvers = resolvers;
        this.defaultClusterName = defaultClusterName;
    }

    @Override
    public PropertyDescriptor getSupportedDynamicPropertyDescriptor(String propertyDescriptorName) {
        for (ClusterResolver resolver : resolvers) {
            final PropertyDescriptor descriptor = resolver.getSupportedDynamicPropertyDescriptor(propertyDescriptorName);
            if (descriptor != null) {
                return descriptor;
            }
        }
        return null;
    }

    @Override
    public Collection<ValidationResult> validate(ValidationContext validationContext) {
        Collection<ValidationResult> results = new ArrayList<>();
        for (ClusterResolver resolver : resolvers) {
            results.addAll(resolver.validate(validationContext));
        }
        return results;
    }

    @Override
    public void configure(PropertyContext context) {
        for (ClusterResolver resolver : resolvers) {
            resolver.configure(context);
        }
    }

    @Override
    // TODO: refactor this, to accept multiple host names at once. Even more accept a comma separated hostname list. And if it contains port number, remove it?
    public String fromHostname(String hostname) {
        for (ClusterResolver resolver : resolvers) {
            final String clusterName = resolver.fromHostname(hostname);
            if (clusterName != null && !clusterName.isEmpty()) {
                return clusterName;
            }
        }
        return defaultClusterName;
    }

    @Override
    public String fromHints(Map<String, String> hints) {
        for (ClusterResolver resolver : resolvers) {
            final String clusterName = resolver.fromHints(hints);
            if (clusterName != null && !clusterName.isEmpty()) {
                return clusterName;
            }
        }
        return defaultClusterName;
    }
}
