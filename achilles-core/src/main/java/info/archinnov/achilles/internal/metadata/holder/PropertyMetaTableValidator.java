package info.archinnov.achilles.internal.metadata.holder;

import com.datastax.driver.core.TableMetadata;
import info.archinnov.achilles.internal.validation.Validator;

public class PropertyMetaTableValidator extends PropertyMetaView{

    protected PropertyMetaTableValidator(PropertyMeta meta) {
        super(meta);
    }

    public void validatePrimaryKeyComponents(TableMetadata tableMetadata, boolean partitionKey) {
        Validator.validateNotNull(meta.embeddedIdProperties, "Cannot validate compound primary keys components against Cassandra meta data because entity '%s' does not have a compound primary key", meta.entityClassName);
        meta.embeddedIdProperties.validatePrimaryKeyComponents(tableMetadata, partitionKey);
    }
}
