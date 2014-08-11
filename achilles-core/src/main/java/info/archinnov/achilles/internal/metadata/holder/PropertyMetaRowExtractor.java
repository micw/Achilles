package info.archinnov.achilles.internal.metadata.holder;

import com.datastax.driver.core.Row;
import info.archinnov.achilles.internal.validation.Validator;

import java.util.List;

public class PropertyMetaRowExtractor extends PropertyMetaView{

    PropertyMetaRowExtractor(PropertyMeta meta) {
        super(meta);
    }

    public List<Object> extractRawCompoundPrimaryComponentsFromRow(Row row) {
        Validator.validateNotNull(meta.embeddedIdProperties, "Cannot extract raw compound primary keys from CQL3 row because entity '%s' does not have a compound primary key", meta.entityClassName);
        return meta.embeddedIdProperties.extractRawCompoundPrimaryComponentsFromRow(row);
    }

    public void validateExtractedCompoundPrimaryComponents(List<Object> rawComponents, Class<?> primaryKeyClass) {
        Validator.validateNotNull(meta.embeddedIdProperties, "Cannot validate raw compound primary keys from CQL3 row because entity '%s' does not have a compound primary key",meta.entityClassName);
        meta.embeddedIdProperties.validateExtractedCompoundPrimaryComponents(rawComponents, primaryKeyClass);
    }

}
