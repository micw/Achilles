package info.archinnov.achilles.internal.metadata.holder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class PropertyMetaSliceQueryContext extends PropertyMetaView {

    private static final Logger log = LoggerFactory.getLogger(PropertyMetaSliceQueryContext.class);

    protected PropertyMetaSliceQueryContext(PropertyMeta meta) {
        super(meta);
    }

    public Object instantiateEmbeddedIdWithPartitionComponents(List<Object> partitionComponents) {
        log.trace("Instantiate compound primary class {} with partition key components {}", meta.valueClass.getCanonicalName(), partitionComponents);
        Object newPrimaryKey = meta.instantiate();
        meta.embeddedIdProperties.copyPartitionComponentsToObject(newPrimaryKey, partitionComponents);
        return newPrimaryKey;
    }
}
