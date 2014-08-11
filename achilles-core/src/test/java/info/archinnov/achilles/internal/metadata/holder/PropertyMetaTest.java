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

import static info.archinnov.achilles.internal.metadata.holder.PropertyType.*;
import static info.archinnov.achilles.type.ConsistencyLevel.QUORUM;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

import info.archinnov.achilles.internal.reflection.ReflectionInvoker;
import info.archinnov.achilles.json.DefaultJacksonMapperFactory;
import info.archinnov.achilles.test.mapping.entity.CompleteBean;
import info.archinnov.achilles.test.parser.entity.EmbeddedKey;
import info.archinnov.achilles.type.ConsistencyLevel;
import info.archinnov.achilles.type.Pair;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.commons.lang.math.RandomUtils;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.common.collect.Sets;

@RunWith(MockitoJUnitRunner.class)
public class PropertyMetaTest {

	@Rule
	public ExpectedException exception = ExpectedException.none();

	private ObjectMapper defaultJakcsonMapper = new DefaultJacksonMapperFactory().getMapper(String.class);

	@Mock
	private ReflectionInvoker invoker;

	@Test
	public void should_return_true_for_isCounter_when_type_is_counter() throws Exception {
		PropertyMeta propertyMeta = PropertyMetaTestBuilder.keyValueClass(Void.class, String.class)
				.type(PropertyType.COUNTER).build();

		assertThat(propertyMeta.structure().isCounter()).isTrue();
	}

	@Test
	public void should_use_equals_and_hashcode() throws Exception {
		PropertyMeta meta1 = new PropertyMeta();
		meta1.setEntityClassName("entity");
		meta1.setPropertyName("field1");
		meta1.setType(PropertyType.SIMPLE);

		PropertyMeta meta2 = new PropertyMeta();
		meta2.setEntityClassName("entity");
		meta2.setPropertyName("field2");
		meta2.setType(PropertyType.SIMPLE);

		PropertyMeta meta3 = new PropertyMeta();
		meta3.setEntityClassName("entity");
		meta3.setPropertyName("field1");
		meta3.setType(PropertyType.LIST);

		PropertyMeta meta4 = new PropertyMeta();
		meta4.setEntityClassName("entity1");
		meta4.setPropertyName("field1");
		meta4.setType(PropertyType.SIMPLE);

		PropertyMeta meta5 = new PropertyMeta();
		meta5.setEntityClassName("entity");
		meta5.setPropertyName("field1");
		meta5.setType(PropertyType.SIMPLE);

		assertThat(meta1).isNotEqualTo(meta2);
		assertThat(meta1).isNotEqualTo(meta3);
		assertThat(meta1).isNotEqualTo(meta4);
		assertThat(meta1).isEqualTo(meta5);

		assertThat(meta1.hashCode()).isNotEqualTo(meta2.hashCode());
		assertThat(meta1.hashCode()).isNotEqualTo(meta3.hashCode());
		assertThat(meta1.hashCode()).isNotEqualTo(meta4.hashCode());
		assertThat(meta1.hashCode()).isEqualTo(meta5.hashCode());

		Set<PropertyMeta> pms = Sets.newHashSet(meta1, meta2, meta3, meta4, meta5);

		assertThat(pms).containsOnly(meta1, meta2, meta3, meta4);
	}

	@Test
	public void should_serialize_as_json() throws Exception {
		PropertyMeta pm = new PropertyMeta();
		pm.setType(SIMPLE);

		assertThat(pm.forTranscoding().forceEncodeToJSON(new UUID(10, 10))).isEqualTo("\"00000000-0000-000a-0000-00000000000a\"");
	}

	@Test
	public void should_return_true_for_is_embedded_id() throws Exception {
		PropertyMeta idMeta = PropertyMetaTestBuilder.valueClass(EmbeddedKey.class).type(EMBEDDED_ID).build();

		assertThat(idMeta.structure().isEmbeddedId()).isTrue();
	}

	@Test
	public void should_return_false_for_is_embedded_id() throws Exception {
		PropertyMeta idMeta = PropertyMetaTestBuilder.valueClass(EmbeddedKey.class).type(ID).build();

		assertThat(idMeta.structure().isEmbeddedId()).isFalse();
	}

	@Test
	public void should_get_read_consistency() throws Exception {
		PropertyMeta pm = new PropertyMeta();

		assertThat(pm.structure().getReadConsistencyLevel()).isNull();
		assertThat(pm.structure().getWriteConsistencyLevel()).isNull();

		pm.setConsistencyLevels(Pair.<ConsistencyLevel, ConsistencyLevel> create(QUORUM, QUORUM));

		assertThat(pm.structure().getReadConsistencyLevel()).isEqualTo(QUORUM);
		assertThat(pm.structure().getWriteConsistencyLevel()).isEqualTo(QUORUM);
	}

	@Test
	public void should_decode() throws Exception {
		PropertyMeta pm = new PropertyMeta();

		assertThat(pm.forTranscoding().decode((Object) null)).isNull();
		assertThat(pm.forTranscoding().decode((List<?>) null)).isNull();
		assertThat(pm.forTranscoding().decode((Set<?>) null)).isNull();
		assertThat(pm.forTranscoding().decode((Map<?, ?>) null)).isNull();
		assertThat(pm.forTranscoding().decodeFromComponents((List<?>) null)).isNull();

		Object value = "";
		List<Object> list = new ArrayList<Object>();
		Set<Object> set = new HashSet<Object>();
		Map<Object, Object> map = new HashMap<Object, Object>();

//		when(transcoder.decode(pm, value)).thenReturn(value);
//		when(transcoder.decodeKey(pm, value)).thenReturn(value);
//		when(transcoder.decode(pm, list)).thenReturn(list);
//		when(transcoder.decode(pm, set)).thenReturn(set);
//		when(transcoder.decode(pm, map)).thenReturn(map);
//		when(transcoder.decodeFromComponents(pm, list)).thenReturn(list);

		assertThat(pm.forTranscoding().decode(value)).isEqualTo(value);
		assertThat(pm.forTranscoding().decode(list)).isEqualTo(list);
		assertThat(pm.forTranscoding().decode(set)).isEqualTo(set);
		assertThat(pm.forTranscoding().decode(map)).isEqualTo(map);
		assertThat(pm.forTranscoding().decodeFromComponents(list)).isEqualTo(list);
	}

	@Test
	public void should_encode() throws Exception {
		PropertyMeta pm = new PropertyMeta();

		assertThat(pm.forTranscoding().encodeToCassandra((Object) null)).isNull();
		assertThat(pm.forTranscoding().encodeToCassandra((List<?>) null)).isNull();
		assertThat(pm.forTranscoding().encodeToCassandra((Set<?>) null)).isNull();
		assertThat(pm.forTranscoding().encodeToCassandra((Map<?, ?>) null)).isNull();
		assertThat(pm.forTranscoding().encodeToComponents( null, true)).isNull();

		Object value = "";
		List<Object> list = new ArrayList<Object>();
		Set<Object> set = new HashSet<Object>();
		Map<Object, Object> map = new HashMap<Object, Object>();

//		when(transcoder.encode(pm, value)).thenReturn(value);
//		when(transcoder.encodeKey(pm, value)).thenReturn(value);
//		when(transcoder.encode(pm, list)).thenReturn(list);
//		when(transcoder.encode(pm, set)).thenReturn(set);
//		when(transcoder.encode(pm, map)).thenReturn(map);
//		when(transcoder.encodeToComponents(pm, list)).thenReturn(list);
//		when(transcoder.encodeToComponents(pm, list)).thenReturn(list);

		assertThat(pm.forTranscoding().encodeToCassandra(value)).isEqualTo(value);
		assertThat(pm.forTranscoding().encodeToCassandra(list)).isEqualTo(list);
		assertThat(pm.forTranscoding().encodeToCassandra(set)).isEqualTo(set);
		assertThat(pm.forTranscoding().encodeToCassandra(map)).isEqualTo(map);
		assertThat(pm.forTranscoding().encodeToComponents(list, false)).isEqualTo(list);
		assertThat(pm.forTranscoding().encodeToComponents(list, false)).isEqualTo(list);
	}

	@Test
	public void should_force_encode_to_json() throws Exception {
		PropertyMeta pm = new PropertyMeta();

		pm.forTranscoding().forceEncodeToJSON("value");

//		verify(transcoder).forceEncodeToJSON("value");
	}

	@Test
	public void should_get_primary_key() throws Exception {

		long id = RandomUtils.nextLong();
		CompleteBean entity = new CompleteBean(id);

		PropertyMeta pm = new PropertyMeta();
		pm.setType(ID);
		pm.setInvoker(invoker);

		when(invoker.getPrimaryKey(entity, pm)).thenReturn(id);

		assertThat(pm.getPrimaryKey(entity)).isEqualTo(id);
	}

	@Test
	public void should_exception_when_asking_primary_key_on_non_id_field() throws Exception {

		CompleteBean entity = new CompleteBean();

		PropertyMeta pm = new PropertyMeta();
		pm.setPropertyName("property");
		pm.setType(SIMPLE);

		exception.expect(IllegalStateException.class);
		exception.expectMessage("Cannot get primary key on a non id field 'property'");

		pm.getPrimaryKey(entity);
	}

	@Test
	public void should_get_value_from_field() throws Exception {

		CompleteBean entity = new CompleteBean();
		entity.setName("name");

		PropertyMeta pm = PropertyMetaTestBuilder.completeBean(Void.class, String.class).field("name").accessors()
				.type(SIMPLE).invoker(invoker).build();

		when(invoker.getValueFromField(entity, pm.getField())).thenReturn("name");

		assertThat(pm.getValueFromField(entity)).isEqualTo("name");
	}

	@Test
	public void should_set_value_to_field() throws Exception {

		CompleteBean entity = new CompleteBean();

		PropertyMeta pm = PropertyMetaTestBuilder.completeBean(Void.class, String.class).field("name").accessors()
				.type(SIMPLE).invoker(invoker).build();

		pm.setValueToField(entity, "name");

		verify(invoker).setValueToField(entity, pm.getField(), "name");
	}

    @Test
    public void should_get_default_empty_list_as_value() throws Exception {
        //Given
        PropertyMeta pm = PropertyMetaTestBuilder.valueClass(String.class).type(LIST).build();
        pm.setEmptyCollectionAndMapIfNull(true);

        //When
        final Object actual = pm.nullValueForCollectionAndMap();

        //Then
        assertThat(actual).isNotNull().isInstanceOf(List.class);
    }

    @Test
    public void should_get_default_empty_set_as_value() throws Exception {
        //Given
        PropertyMeta pm = PropertyMetaTestBuilder.valueClass(String.class).type(SET).build();
        pm.setEmptyCollectionAndMapIfNull(true);

        //When
        final Object actual = pm.nullValueForCollectionAndMap();

        //Then
        assertThat(actual).isNotNull().isInstanceOf(Set.class);
    }

    @Test
    public void should_get_default_empty_map_as_value() throws Exception {
        //Given
        PropertyMeta pm = PropertyMetaTestBuilder.keyValueClass(String.class,Object.class).type(MAP).build();
        pm.setEmptyCollectionAndMapIfNull(true);

        //When
        final Object actual = pm.nullValueForCollectionAndMap();

        //Then
        assertThat(actual).isNotNull().isInstanceOf(Map.class);
    }
}
