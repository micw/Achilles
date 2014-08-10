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
package info.archinnov.achilles.internal.metadata.holder;

import static info.archinnov.achilles.schemabuilder.Create.Options.ClusteringOrder;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import info.archinnov.achilles.exception.AchillesException;
import info.archinnov.achilles.internal.validation.Validator;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Objects;

import javax.sql.rowset.Predicate;

public class EmbeddedIdProperties extends AbstractComponentProperties {

	private static final Logger log = LoggerFactory.getLogger(EmbeddedIdProperties.class);

    private final PartitionComponents partitionComponents;
	private final ClusteringComponents clusteringComponents;
    private final String entityClassName;
	public EmbeddedIdProperties(PartitionComponents partitionComponents, ClusteringComponents clusteringComponents, List<PropertyMeta> keyMetas, String entityClassName) {
		super(keyMetas);
		this.partitionComponents = partitionComponents;
		this.clusteringComponents = clusteringComponents;
        this.entityClassName = entityClassName;
    }

	void validatePartitionComponents(String className,Object...partitionComponents) {
		this.partitionComponents.validatePartitionComponents(className, partitionComponents);
	}

    void validatePartitionComponentsIn(String className,Object...partitionComponents) {
        this.partitionComponents.validatePartitionComponentsIn(className, partitionComponents);
    }

	void validateClusteringComponents(String className, Object...clusteringComponents) {
		this.clusteringComponents.validateClusteringComponents(className, clusteringComponents);
	}

    void validateClusteringComponentsIn(String className, Object...clusteringComponents) {
        this.clusteringComponents.validateClusteringComponentsIn(className, clusteringComponents);
    }

	public boolean isCompositePartitionKey() {
		return partitionComponents.isComposite();
	}

	public boolean isClustered() {
		return clusteringComponents.isClustered();
	}

	public String getOrderingComponent() {
		return clusteringComponents.getOrderingComponent();
	}

	public List<ClusteringOrder> getCluseringOrders() {
		return clusteringComponents.getClusteringOrders();
	}

	public List<String> getClusteringComponentNames() {
		return clusteringComponents.getComponentNames();
	}

	public List<Class<?>> getClusteringComponentClasses() {
		return clusteringComponents.getComponentClasses();
	}

	public List<String> getPartitionComponentNames() {
		return partitionComponents.getComponentNames();
	}

	public List<Class<?>> getPartitionComponentClasses() {
		return partitionComponents.getComponentClasses();
	}

	public List<Field> getPartitionComponentFields() {
		return partitionComponents.getComponentFields();
	}

	@Override
	public String toString() {

		return Objects.toStringHelper(this.getClass()).add("partitionComponents", partitionComponents)
				.add("clusteringComponents", clusteringComponents).toString();

	}

    public Object decodeFromComponents(Object newInstance, List<?> components) {
        log.trace("Decode from CQL components {}", components);
        Validator.validateTrue(components.size() == this.propertyMetas.size(), "There should be exactly '%s' Cassandra columns to decode into an '%s' instance", this.propertyMetas.size(), newInstance.getClass().getCanonicalName());
        for (int i = 0; i < propertyMetas.size(); i++) {
            final PropertyMeta componentMeta = propertyMetas.get(i);
            final Object decodedValue = componentMeta.decodeFromCassandra(components.get(i));
            componentMeta.setValueToField(newInstance,decodedValue);
        }
        return newInstance;
    }

    public List<Object> encodeToComponents(Object compoundKey, boolean onlyStaticColumns) {
        log.trace("Encode {} to CQL components with 'onlyStaticColumns' : {}", compoundKey, onlyStaticColumns);
        List<Object> encoded = new ArrayList<>();
        if (onlyStaticColumns) {
            for (PropertyMeta partitionKeyMeta : partitionComponents.propertyMetas) {
                encoded.add(partitionKeyMeta.getAndEncodeValueForCassandra(compoundKey));
            }
        } else {
            for (PropertyMeta partitionKeyMeta : propertyMetas) {
                encoded.add(partitionKeyMeta.getAndEncodeValueForCassandra(compoundKey));
            }
        }
        return encoded;
    }

    public List<Object> encodePartitionComponents(List<Object> rawPartitionComponents) {
        log.trace("Encode {} to CQL partition components", rawPartitionComponents);
        Validator.validateTrue(rawPartitionComponents.size() <= partitionComponents.propertyMetas.size(),"There should be no more than '%s' partition components to be encoded for class '%s'", rawPartitionComponents, entityClassName);
        return encodeElements(rawPartitionComponents, partitionComponents.propertyMetas);
    }

    public List<Object> encodePartitionComponentsIN(List<Object> rawPartitionComponentsIN) {
        log.trace("Encode {} to CQL partition components IN", rawPartitionComponentsIN);
        final List<PropertyMeta> partitionComponentMetas = partitionComponents.propertyMetas;
        final PropertyMeta lastPartitionComponentMeta = partitionComponentMetas.get(partitionComponentMetas.size() - 1);
        return encodeLastComponent(rawPartitionComponentsIN, lastPartitionComponentMeta);
    }

    public List<Object> encodeClusteringKeys(List<Object> rawClusteringKeys) {
        log.trace("Encode {} to CQL clustering keys", rawClusteringKeys);
        Validator.validateTrue(rawClusteringKeys.size() <= clusteringComponents.propertyMetas.size(),"There should be no more than '%s' clustering components to be encoded for class '%s'", rawClusteringKeys, entityClassName);
        return encodeElements(rawClusteringKeys, clusteringComponents.propertyMetas);
    }

    public List<Object> encodeClusteringKeysIN(List<Object> rawClusteringKeysIN) {
        log.trace("Encode {} to CQL clustering keys IN", rawClusteringKeysIN);
        final List<PropertyMeta> cluseringKeyMetas = clusteringComponents.propertyMetas;
        final PropertyMeta lastClusteringKeyMeta = cluseringKeyMetas.get(cluseringKeyMetas.size() - 1);
        return encodeLastComponent(rawClusteringKeysIN, lastClusteringKeyMeta);
    }

    private List<Object> encodeElements(List<Object> rawPartitionComponents, List<PropertyMeta> propertyMetas) {
        List<Object> encoded = new ArrayList<>();
        for (int i = 0; i < propertyMetas.size(); i++) {
            final PropertyMeta componentMeta = propertyMetas.get(i);
            encoded.add(componentMeta.encodeToCassandra(rawPartitionComponents.get(i)));
        }
        return encoded;
    }

    private List<Object> encodeLastComponent(List<Object> rawPartitionComponentsIN, PropertyMeta lastComponentMeta) {
        List<Object> encoded = new ArrayList<>();
        for (Object rawPartitionComponentIN : rawPartitionComponentsIN) {
            encoded.add(lastComponentMeta.encode(rawPartitionComponentIN));
        }
        return encoded;
    }


}
