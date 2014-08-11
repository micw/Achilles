package info.archinnov.achilles.internal.metadata.holder;

import com.google.common.collect.Sets;

import java.util.HashSet;
import java.util.Set;

public class PropertyMetaCacheSupport extends PropertyMetaView{

    protected PropertyMetaCacheSupport(PropertyMeta meta) {
        super(meta);
    }

    public Set<String> extractClusteredFieldsIfNecessary() {
        if (meta.structure().isEmbeddedId()) {
            return new HashSet<>(meta.embeddedIdProperties.getCQL3ComponentNames());
        } else {
            return Sets.newHashSet(meta.getCQL3PropertyName());
        }
    }
}
