package info.archinnov.achilles.internal.metadata.holder;

import info.archinnov.achilles.internal.validation.Validator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class PropertyMetaTypedQuery extends PropertyMetaView {

    private static final Logger log = LoggerFactory.getLogger(PropertyMetaTypedQuery.class);

    protected PropertyMetaTypedQuery(PropertyMeta meta) {
        super(meta);
    }

    public void validateTypedQuery(String queryString) {
        log.trace("Validate typed query string {}", queryString);
        if (meta.structure().isEmbeddedId()) {
            meta.embeddedIdProperties.validateTypedQuery(queryString, meta.valueClass);
        } else {
            Validator.validateTrue(queryString.contains(meta.getCQL3PropertyName()), "The typed query [%s] should contain the id column '%s'", queryString, meta.getCQL3PropertyName());
        }
    }
}
