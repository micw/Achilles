/*
 * Copyright (C) 2012-2014 DuyHai DOAN
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package info.archinnov.achilles.internal.reflection;

import static info.archinnov.achilles.internal.cql.TypeMapper.getRowMethod;
import static info.archinnov.achilles.internal.cql.TypeMapper.toCompatibleJavaType;
import static info.archinnov.achilles.internal.metadata.holder.EntityMeta.EntityState;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.datastax.driver.core.ColumnDefinitions.Definition;
import com.datastax.driver.core.Row;
import info.archinnov.achilles.exception.AchillesException;
import info.archinnov.achilles.internal.metadata.holder.EntityMeta;
import info.archinnov.achilles.internal.metadata.holder.PropertyMeta;
import info.archinnov.achilles.internal.validation.Validator;

public class RowMethodInvoker {
    private static final Logger log = LoggerFactory.getLogger(RowMethodInvoker.class);

    public Object invokeOnRowForFields(Row row, PropertyMeta pm) {
        String propertyName = pm.getPropertyName().toLowerCase();
        Object value = null;
        if (!row.isNull(propertyName)) {
            switch (pm.type()) {
                case LIST:
                    value = invokeOnRowForList(row, pm, propertyName, pm.getValueClass());
                    break;
                case SET:
                    value = invokeOnRowForSet(row, pm, propertyName, pm.getValueClass());
                    break;
                case MAP:
                    Class<?> keyClass = pm.getKeyClass();
                    Class<?> valueClass = pm.getValueClass();
                    value = invokeOnRowForMap(row, pm, propertyName, keyClass, valueClass);
                    break;
                case ID:
                case SIMPLE:
                    value = invokeOnRowForProperty(row, pm, propertyName, pm.getValueClass());
                    break;
                default:
                    break;
            }
        } else {
            switch (pm.type()) {
                case LIST:
                case SET:
                case MAP:
                    value = pm.nullValueForCollectionAndMap();
                    break;
                default:
                    break;
            }
        }
        return value;
    }

    public Object extractCompoundPrimaryKeyFromRow(Row row, EntityMeta meta,PropertyMeta pm, EntityState entityState) {
        log.trace("Extract compound primary key {} from CQL row for entity class {}", pm.getPropertyName(),pm.getEntityClassName());
        final List<Object> rawComponents = pm.extractRawCompoundPrimaryComponentsFromRow(row);
        if (entityState.isManaged() && !meta.hasOnlyStaticColumns()) {
            pm.validateExtractedCompoundPrimaryComponents(rawComponents, pm.getValueClass());
        }
        return pm.decodeFromComponents(rawComponents);

    }

    private Object invokeOnRowForProperty(Row row, PropertyMeta pm, String propertyName, Class<?> valueClass) {
        log.trace("Extract property {} from CQL row for entity class {}", propertyName, pm.getEntityClassName());
        try {
            Object rawValue = getRowMethod(valueClass).invoke(row, propertyName);
            return pm.decode(rawValue);
        } catch (Exception e) {
            throw new AchillesException("Cannot retrieve property '" + propertyName + "' for entity class '"
                    + pm.getEntityClassName() + "' from CQL Row", e);
        }
    }

    public List<?> invokeOnRowForList(Row row, PropertyMeta pm, String propertyName, Class<?> valueClass) {
        log.trace("Extract list property {} from CQL row for entity class {}", propertyName, pm.getEntityClassName());
        try {
            List<?> rawValues = row.getList(propertyName, toCompatibleJavaType(valueClass));
            return pm.decode(rawValues);

        } catch (Exception e) {
            throw new AchillesException("Cannot retrieve list property '" + propertyName + "' from CQL Row", e);
        }
    }

    public Set<?> invokeOnRowForSet(Row row, PropertyMeta pm, String propertyName, Class<?> valueClass) {
        log.trace("Extract set property {} from CQL row for entity class {}", propertyName, pm.getEntityClassName());
        try {
            Set<?> rawValues = row.getSet(propertyName, toCompatibleJavaType(valueClass));
            return pm.decode(rawValues);

        } catch (Exception e) {
            throw new AchillesException("Cannot retrieve set property '" + propertyName + "' from CQL Row", e);
        }
    }

    public Map<?, ?> invokeOnRowForMap(Row row, PropertyMeta pm, String propertyName, Class<?> keyClass,
            Class<?> valueClass) {
        log.trace("Extract map property {} from CQL row for entity class {}", propertyName, pm.getEntityClassName());
        try {
            Map<?, ?> rawValues = row.getMap(propertyName, toCompatibleJavaType(keyClass),
                    toCompatibleJavaType(valueClass));
            return pm.decode(rawValues);

        } catch (Exception e) {
            throw new AchillesException("Cannot retrieve map property '" + propertyName + "' from CQL Row", e);
        }
    }

    @SuppressWarnings("unchecked")
    public <T> T invokeOnRowForType(Row row, Class<T> type, String name) {
        log.trace("Extract property {} of type {} from CQL row ", name, type);
        try {
            return (T) getRowMethod(type).invoke(row, name);
        } catch (Exception e) {
            throw new AchillesException("Cannot retrieve column '" + name + "' of type '" + type.getCanonicalName()
                    + "' from CQL Row", e);
        }
    }
}
