package info.archinnov.achilles.internal.metadata.holder;

import info.archinnov.achilles.exception.AchillesException;
import info.archinnov.achilles.internal.persistence.operations.InternalCounterImpl;
import info.archinnov.achilles.internal.validation.Validator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class PropertyMetaTranscoder extends PropertyMetaView {

    private static final Logger log = LoggerFactory.getLogger(PropertyMetaTranscoder.class);

    protected PropertyMetaTranscoder(PropertyMeta meta) {
        super(meta);
    }

    public Object decode(Object cassandraValue) {
        return cassandraValue == null ? null : meta.simpleCodec.decode(cassandraValue);
    }

    public List<Object> decode(List<?> cassandraValue) {
        return cassandraValue == null ? null : meta.listCodec.decode(cassandraValue);
    }

    public Set<Object> decode(Set<?> cassandraValue) {
        return cassandraValue == null ? null : meta.setCodec.decode(cassandraValue);
    }

    public Map<Object, Object> decode(Map<?, ?> cassandraValue) {
        return cassandraValue == null ? null : meta.mapCodec.decode(cassandraValue);
    }

    public Object decodeFromComponents(List<?> components) {
        Validator.validateTrue(meta.type == PropertyType.EMBEDDED_ID, "Cannot decode components '%s' for the property '%s' which is not a compound primary key", components, meta.propertyName);
        return components == null ? null : meta.embeddedIdProperties.decodeFromComponents(meta.instantiate(), components);
    }

    Object decodeFromCassandra(Object fromCassandra) {
        switch (meta.type) {
            case SIMPLE:
            case ID:
                return meta.simpleCodec.decode(fromCassandra);
            case LIST:
                return meta.listCodec.decode((List)fromCassandra);
            case SET:
                return meta.setCodec.decode((Set)fromCassandra);
            case MAP:
                return meta.mapCodec.decode((Map)fromCassandra);
            default:
                throw new AchillesException(String.format("Cannot decode value '%s' from CQL3 for property '%s' of type '%s'",fromCassandra, meta.propertyName, meta.type.name()));
        }
    }

    public <T> T encodeToCassandra(Object fromJava) {
        switch (meta.type) {
            case SIMPLE:
            case ID:
                return (T)meta.simpleCodec.encode(fromJava);
            case LIST:
                return (T)meta.listCodec.encode((List) fromJava);
            case SET:
                return (T)meta.setCodec.encode((Set) fromJava);
            case MAP:
                return (T)meta.mapCodec.encode((Map) fromJava);
            case COUNTER:
                return (T)((InternalCounterImpl) fromJava).getInternalCounterDelta();
            default:
                throw new AchillesException(String.format("Cannot encode value '%s' to CQL3 for property '%s' of type '%s'",fromJava, meta.propertyName, meta.type.name()));
        }
    }

    public Object getAndEncodeValueForCassandra(Object entity) {
        Object value = meta.getValueFromField(entity);
        if (value != null) {
            return encodeToCassandra(value);
        } else {
            return null;
        }
    }


    public List<Object> encodeToComponents(Object compoundKey, boolean onlyStaticColumns) {
        Validator.validateTrue(meta.type == PropertyType.EMBEDDED_ID, "Cannot encode object '%s' for the property '%s' which is not a compound primary key", compoundKey, meta.propertyName);
        return compoundKey == null ? null : meta.embeddedIdProperties.encodeToComponents(compoundKey, onlyStaticColumns);
    }


    public String forceEncodeToJSON(Object object) {
        log.trace("Force encode {} to JSON", object);
        Validator.validateNotNull(object, "Cannot encode to JSON null primary key for class '%s'", meta.entityClassName);
        if (object instanceof String) {
            return String.class.cast(object);
        } else {
            try {
                return this.meta.defaultJacksonMapper.writeValueAsString(object);
            } catch (Exception e) {
                throw new AchillesException(String.format("Error while encoding primary key '%s' for class '%s'", object, meta.entityClassName), e);
            }
        }
    }
}
