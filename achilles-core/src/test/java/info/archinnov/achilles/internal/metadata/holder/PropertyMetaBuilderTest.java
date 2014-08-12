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
import static info.archinnov.achilles.type.ConsistencyLevel.*;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import info.archinnov.achilles.internal.metadata.transcoding.codec.ListCodec;
import info.archinnov.achilles.internal.metadata.transcoding.codec.MapCodec;
import info.archinnov.achilles.internal.metadata.transcoding.codec.SetCodec;
import info.archinnov.achilles.internal.metadata.transcoding.codec.SimpleCodec;
import info.archinnov.achilles.test.mapping.entity.CompleteBean;
import info.archinnov.achilles.test.parser.entity.Bean;
import info.archinnov.achilles.test.parser.entity.EmbeddedKey;
import info.archinnov.achilles.type.Pair;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.List;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Before;
import org.junit.Test;

public class PropertyMetaBuilderTest {
	private Method[] accessors = new Method[2];
    private Field field;

	private ObjectMapper objectMapper = new ObjectMapper();

	@Before
	public void setUp() throws Exception {
		accessors[0] = Bean.class.getDeclaredMethod("getId");
		accessors[1] = Bean.class.getDeclaredMethod("setId", Long.class);
        field = CompleteBean.class.getDeclaredField("id");
	}

	@Test
	public void should_build_simple() throws Exception {

		PropertyMeta built = PropertyMetaBuilder.factory().type(SIMPLE).propertyName("prop").accessors(accessors)
                .field(field).objectMapper(objectMapper).consistencyLevels(Pair.create(ONE, ALL)).build(Void.class, String.class);

		assertThat(built.type()).isEqualTo(SIMPLE);
		assertThat(built.getPropertyName()).isEqualTo("prop");

		assertThat(built.<String> getValueClass()).isEqualTo(String.class);

		assertThat(built.getField()).isEqualTo(field);
		assertThat(built.structure().isEmbeddedId()).isFalse();
		assertThat(built.structure().getReadConsistencyLevel()).isEqualTo(ONE);
		assertThat(built.structure().getWriteConsistencyLevel()).isEqualTo(ALL);
		assertThat(built.simpleCodec).isInstanceOf(SimpleCodec.class);
	}

	@Test
	public void should_build_compound_id() throws Exception {

        EmbeddedIdProperties props = mock(EmbeddedIdProperties.class);

        PropertyMeta built = PropertyMetaBuilder.factory().type(EMBEDDED_ID).propertyName("prop").accessors(accessors)
                .objectMapper(objectMapper).consistencyLevels(Pair.create(ONE, ALL)).embeddedIdProperties(props)
                .build(Void.class, EmbeddedKey.class);

        assertThat(built.type()).isEqualTo(EMBEDDED_ID);
        assertThat(built.getPropertyName()).isEqualTo("prop");

        assertThat(built.<EmbeddedKey>getValueClass()).isEqualTo(EmbeddedKey.class);

        assertThat(built.structure().isEmbeddedId()).isTrue();
        assertThat(built.structure().getReadConsistencyLevel()).isEqualTo(ONE);
        assertThat(built.structure().getWriteConsistencyLevel()).isEqualTo(ALL);
        assertThat(built.simpleCodec).isNull();
    }


	@Test
	public void should_build_simple_with_object_as_value() throws Exception {
		PropertyMeta built = PropertyMetaBuilder.factory().type(SIMPLE).propertyName("prop").accessors(accessors)
				.objectMapper(objectMapper).build(Void.class, Bean.class);

		assertThat(built.type()).isEqualTo(SIMPLE);
		assertThat(built.getPropertyName()).isEqualTo("prop");

		assertThat(built.<Bean> getValueClass()).isEqualTo(Bean.class);

		assertThat(built.structure().isEmbeddedId()).isFalse();
		assertThat(built.simpleCodec).isInstanceOf(SimpleCodec.class);
	}

	@Test
	public void should_build_list_with_default_empty_when_null() throws Exception {

		PropertyMeta built = PropertyMetaBuilder.factory().type(LIST).propertyName("prop").accessors(accessors)
				.objectMapper(objectMapper).emptyCollectionAndMapIfNull(true).build(Void.class, String.class);

		assertThat(built.type()).isEqualTo(LIST);
		assertThat(built.getPropertyName()).isEqualTo("prop");

		assertThat(built.<String> getValueClass()).isEqualTo(String.class);

		assertThat(built.structure().isEmbeddedId()).isFalse();
		assertThat(built.nullValueForCollectionAndMap()).isNotNull().isInstanceOf(List.class);
		assertThat(built.listCodec).isInstanceOf(ListCodec.class);
	}

	@Test
	public void should_build_set() throws Exception {

		PropertyMeta built = PropertyMetaBuilder.factory().type(SET).propertyName("prop").accessors(accessors)
				.objectMapper(objectMapper).build(Void.class, String.class);

		assertThat(built.type()).isEqualTo(SET);
		assertThat(built.getPropertyName()).isEqualTo("prop");

		assertThat(built.<String> getValueClass()).isEqualTo(String.class);

		assertThat(built.structure().isEmbeddedId()).isFalse();
		assertThat(built.setCodec).isInstanceOf(SetCodec.class);
	}

	@Test
	public void should_build_map() throws Exception {

		PropertyMeta built = PropertyMetaBuilder.factory().type(MAP).propertyName("prop").accessors(accessors)
				.objectMapper(objectMapper).build(Integer.class, String.class);

		assertThat(built.type()).isEqualTo(MAP);
		assertThat(built.getPropertyName()).isEqualTo("prop");

		assertThat(built.<Integer> getKeyClass()).isEqualTo(Integer.class);

		assertThat(built.<String> getValueClass()).isEqualTo(String.class);

		assertThat(built.structure().isEmbeddedId()).isFalse();
		assertThat(built.mapCodec).isInstanceOf(MapCodec.class);
	}

	@Test
	public void should_build_map_with_object_as_key() throws Exception {
		PropertyMeta built = PropertyMetaBuilder.factory().type(MAP).propertyName("prop").accessors(accessors)
				.objectMapper(objectMapper).build(Bean.class, String.class);

		assertThat(built.type()).isEqualTo(MAP);
		assertThat(built.getPropertyName()).isEqualTo("prop");

		assertThat(built.<Bean> getKeyClass()).isEqualTo(Bean.class);

		assertThat(built.<String> getValueClass()).isEqualTo(String.class);
        assertThat(built.mapCodec).isInstanceOf(MapCodec.class);

	}
}
