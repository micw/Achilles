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

import info.archinnov.achilles.internal.metadata.transcoding.DataTranscoder;
import info.archinnov.achilles.internal.validation.Validator;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Objects;

public class EmbeddedIdProperties extends AbstractComponentProperties {

	private static final Logger log = LoggerFactory.getLogger(EmbeddedIdProperties.class);

	private final PartitionComponents partitionComponents;
	private final ClusteringComponents clusteringComponents;
	private final List<String> timeUUIDComponents;

	public EmbeddedIdProperties(PartitionComponents partitionComponents, ClusteringComponents clusteringComponents,
			List<Class<?>> componentClasses, List<String> componentNames, List<Field> componentFields,
			List<Method> componentGetters, List<Method> componentSetters, List<String> timeUUIDComponents) {
		super(componentClasses, componentNames, componentFields, componentGetters, componentSetters);
		this.partitionComponents = partitionComponents;
		this.clusteringComponents = clusteringComponents;
		this.timeUUIDComponents = timeUUIDComponents;
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

	public List<String> getTimeUUIDComponents() {
		return timeUUIDComponents;
	}


	List<Object> extractPartitionComponents(List<Object> components) {
		log.trace("Extract partition key components from {}", components);
		int partitionComponentsCount = partitionComponents.getComponentClasses().size();

		Validator.validateTrue(components.size() >= partitionComponentsCount,
				"Cannot extract composite partition key components from components list '%s'", components);
		return components.subList(0, partitionComponentsCount);
	}

	List<Object> extractClusteringComponents(List<Object> components) {
		log.trace("Extract clustering components from {}", components);
		int partitionComponentsCount = partitionComponents.getComponentClasses().size();

		Validator.validateTrue(components.size() >= partitionComponentsCount,
				"Cannot extract clustering components from components list '%s'", components);
		return components.subList(partitionComponentsCount, components.size());
	}

	@Override
	public String toString() {

		return Objects.toStringHelper(this.getClass()).add("partitionComponents", partitionComponents)
				.add("clusteringComponents", clusteringComponents).toString();

	}

}
