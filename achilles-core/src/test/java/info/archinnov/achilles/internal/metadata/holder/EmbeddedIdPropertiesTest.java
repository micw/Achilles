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
import static info.archinnov.achilles.schemabuilder.Create.Options.ClusteringOrder.Sorting.DESC;
import static org.fest.assertions.api.Assertions.assertThat;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import org.junit.Test;

public class EmbeddedIdPropertiesTest {

	private static final List<Class<?>> noClasses = Arrays.asList();
	private static final List<String> noNames = Arrays.asList();
	private static final List<Method> noAccessors = Arrays.asList();
	private static final List<Field> noFields = Arrays.asList();
	private static final List<String> noTimeUUID = Arrays.asList();
	private static final List<ClusteringOrder> noClusteringOrder = Arrays.asList();

	@Test
	public void should_to_string() throws Exception {

		PartitionComponents partitionComponents = new PartitionComponents(Arrays.<Class<?>> asList(Long.class), Arrays.asList("id"),
				noFields, noAccessors, noAccessors);

		ClusteringComponents clusteringComponents = new ClusteringComponents(Arrays.<Class<?>> asList(UUID.class), Arrays.asList("date"),
				noFields, noAccessors, noAccessors, noClusteringOrder);

		EmbeddedIdProperties props = new EmbeddedIdProperties(partitionComponents, clusteringComponents, noClasses, noNames,
				noFields, noAccessors, noAccessors, noTimeUUID);

		StringBuilder toString = new StringBuilder();
		toString.append("EmbeddedIdProperties{");
		toString.append("partitionComponents=PartitionComponents{componentClasses=java.lang.Long, componentNames=[id]}, ");
		toString.append("clusteringComponents=ClusteringComponents{componentClasses=java.util.UUID, componentNames=[date]}}");

		assertThat(props.toString()).isEqualTo(toString.toString());
	}

	@Test
	public void should_get_ordering_component() throws Exception {
		PartitionComponents partitionComponents = new PartitionComponents(Arrays.<Class<?>> asList(Long.class), Arrays.asList("id"),
				noFields, noAccessors, noAccessors);

		ClusteringComponents clusteringComponents = new ClusteringComponents(Arrays.<Class<?>> asList(UUID.class, String.class),
				Arrays.asList("date", "name"), noFields, noAccessors, noAccessors, noClusteringOrder);

		EmbeddedIdProperties props = new EmbeddedIdProperties(partitionComponents, clusteringComponents, noClasses, noNames,
				noFields, noAccessors, noAccessors, noTimeUUID);

		assertThat(props.getOrderingComponent()).isEqualTo("date");
	}
	
	@Test
	public void should_get_reversed_component() throws Exception {

        final ClusteringOrder clusteringOrder = new ClusteringOrder("col", DESC);
        PartitionComponents partitionComponents = new PartitionComponents(Arrays.<Class<?>> asList(Long.class), Arrays.asList("id"),
				noFields, noAccessors, noAccessors);


		ClusteringComponents clusteringComponents = new ClusteringComponents(Arrays.<Class<?>> asList(UUID.class, String.class),
				Arrays.asList("date", "name"),  noFields, noAccessors, noAccessors, Arrays.asList(clusteringOrder));

		EmbeddedIdProperties props = new EmbeddedIdProperties(partitionComponents, clusteringComponents, noClasses, noNames,
				noFields, noAccessors, noAccessors, noTimeUUID);

        assertThat(props.getCluseringOrders()).containsExactly(clusteringOrder);
    }

	@Test
	public void should_return_null_if_no_ordering_component() throws Exception {
		PartitionComponents partitionComponents = new PartitionComponents(Arrays.<Class<?>> asList(Long.class), Arrays.asList("id"),
				noFields, noAccessors, noAccessors);

		ClusteringComponents clusteringComponents = new ClusteringComponents(noClasses, noNames, noFields, noAccessors, noAccessors, noClusteringOrder);

		EmbeddedIdProperties props = new EmbeddedIdProperties(partitionComponents, clusteringComponents, noClasses, noNames,
				noFields, noAccessors, noAccessors, noTimeUUID);

		assertThat(props.getOrderingComponent()).isNull();
	}

	@Test
	public void should_get_clustering_component_names_and_classes() throws Exception {
		PartitionComponents partitionComponents = new PartitionComponents(Arrays.<Class<?>> asList(Long.class), Arrays.asList("id"),
				noFields, noAccessors, noAccessors);

		ClusteringComponents clusteringComponents = new ClusteringComponents(Arrays.<Class<?>> asList(UUID.class, String.class),
				Arrays.asList("date", "name"), noFields, noAccessors, noAccessors, noClusteringOrder);

		EmbeddedIdProperties props = new EmbeddedIdProperties(partitionComponents, clusteringComponents, noClasses, noNames,
				noFields, noAccessors, noAccessors, noTimeUUID);

		assertThat(props.getClusteringComponentNames()).containsExactly("date", "name");
		assertThat(props.getClusteringComponentClasses()).containsExactly(UUID.class, String.class);
		assertThat(props.isClustered()).isTrue();
	}

	@Test
	public void should_get_partition_component_names_and_classes() throws Exception {
		PartitionComponents partitionComponents = new PartitionComponents(Arrays.<Class<?>> asList(Long.class, String.class),
				Arrays.asList("id", "type"), noFields, noAccessors, noAccessors);

		ClusteringComponents clusteringComponents = new ClusteringComponents(Arrays.<Class<?>> asList(UUID.class, String.class),
				Arrays.asList("date", "name"), noFields, noAccessors, noAccessors, noClusteringOrder);

		EmbeddedIdProperties props = new EmbeddedIdProperties(partitionComponents, clusteringComponents, noClasses, noNames,
				noFields, noAccessors, noAccessors, noTimeUUID);

		assertThat(props.getPartitionComponentNames()).containsExactly("id", "type");
		assertThat(props.getPartitionComponentClasses()).containsExactly(Long.class, String.class);

		assertThat(props.isCompositePartitionKey()).isTrue();
	}

	@Test
	public void should_extract_partition_and_clustering_components_for_compound_partition_clustered_entity()
			throws Exception {
		PartitionComponents partitionComponents = new PartitionComponents(Arrays.<Class<?>> asList(Long.class, String.class),
				Arrays.asList("id", "type"), noFields, noAccessors, noAccessors);

		ClusteringComponents clusteringComponents = new ClusteringComponents(Arrays.<Class<?>> asList(UUID.class, String.class),
				Arrays.asList("date", "name"), noFields, noAccessors, noAccessors, noClusteringOrder);

		EmbeddedIdProperties props = new EmbeddedIdProperties(partitionComponents, clusteringComponents, noClasses, noNames,
				noFields, noAccessors, noAccessors, noTimeUUID);

		UUID date = new UUID(10, 10);
		List<Object> components = Arrays.<Object> asList(10L, "type", date, "name");

		assertThat(props.extractPartitionComponents(components)).containsExactly(10L, "type");
		assertThat(props.extractClusteringComponents(components)).containsExactly(date, "name");
	}

	@Test
	public void should_extract_partition_and_clustering_components_for_non_cluster_compound_partition_key()
			throws Exception {
		PartitionComponents partitionComponents = new PartitionComponents(Arrays.<Class<?>> asList(Long.class, String.class),
				Arrays.asList("id", "type"), noFields, noAccessors, noAccessors);

		ClusteringComponents clusteringComponents = new ClusteringComponents(noClasses, noNames, noFields, noAccessors, noAccessors, noClusteringOrder);

		EmbeddedIdProperties props = new EmbeddedIdProperties(partitionComponents, clusteringComponents, noClasses, noNames,
				noFields, noAccessors, noAccessors, noTimeUUID);

		List<Object> components = Arrays.<Object> asList(10L, "type");

		assertThat(props.extractPartitionComponents(components)).containsExactly(10L, "type");
		assertThat(props.extractClusteringComponents(components)).isEmpty();
	}

	@Test
	public void should_extract_partition_and_clustering_components_for_simple_clustered_entity() throws Exception {
		PartitionComponents partitionComponents = new PartitionComponents(Arrays.<Class<?>> asList(Long.class), Arrays.asList("id"),
				noFields, noAccessors, noAccessors);

		ClusteringComponents clusteringComponents = new ClusteringComponents(Arrays.<Class<?>> asList(UUID.class, String.class),
				Arrays.asList("date", "name"), noFields, noAccessors, noAccessors, noClusteringOrder);

		EmbeddedIdProperties props = new EmbeddedIdProperties(partitionComponents, clusteringComponents, noClasses, noNames,
				noFields, noAccessors, noAccessors, noTimeUUID);

		UUID date = new UUID(10, 10);
		List<Object> components = Arrays.<Object> asList(10L, date, "name");

		assertThat(props.extractPartitionComponents(components)).containsExactly(10L);
		assertThat(props.extractClusteringComponents(components)).containsExactly(date, "name");
	}

}
