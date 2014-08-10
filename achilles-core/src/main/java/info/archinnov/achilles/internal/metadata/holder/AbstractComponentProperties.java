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

import static com.google.common.collect.FluentIterable.from;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.List;

import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.base.Predicate;

public abstract class AbstractComponentProperties {

    private static final Function<PropertyMeta, Class<?>> GET_CLASS = new Function<PropertyMeta, Class<?>>() {
        @Override
        public Class<?> apply(PropertyMeta meta) {
            return meta.getValueClass();
        }
    };

    private static final Function<PropertyMeta, String> GET_NAME = new Function<PropertyMeta, String>() {
        @Override
        public String apply(PropertyMeta meta) {
            return meta.getPropertyName();
        }
    };

    private static final Function<PropertyMeta, String> GET_CQL3_NAME = new Function<PropertyMeta, String>() {
        @Override
        public String apply(PropertyMeta meta) {
            return meta.getCQL3PropertyName();
        }
    };

    private static final Function<PropertyMeta, Field> GET_FIELD = new Function<PropertyMeta, Field>() {
        @Override
        public Field apply(PropertyMeta meta) {
            return meta.getField();
        }
    };

    private static final Function<PropertyMeta, Method> GET_GETTER = new Function<PropertyMeta, Method>() {
        @Override
        public Method apply(PropertyMeta meta) {
            return meta.getGetter();
        }
    };

    private static final Function<PropertyMeta, Method> GET_SETTER = new Function<PropertyMeta, Method>() {
        @Override
        public Method apply(PropertyMeta meta) {
            return meta.getSetter();
        }
    };

    protected final List<PropertyMeta> propertyMetas;

	protected AbstractComponentProperties(List<PropertyMeta> propertyMetas) {
        this.propertyMetas = propertyMetas;
    }

	public List<Class<?>> getComponentClasses() {
		return from(propertyMetas).transform(GET_CLASS).toList();
	}

    public List<Field> getComponentFields() {
        return from(propertyMetas).transform(GET_FIELD).toList();
    }

    public List<String> getComponentNames() {
		return from(propertyMetas).transform(GET_NAME).toList();
	}

    public List<String> getCQL3ComponentNames() {
        return from(propertyMetas).transform(GET_CQL3_NAME).toList();
    }

    public List<Method> getComponentGetters() {
		return from(propertyMetas).transform(GET_GETTER).toList();
	}

    public List<Method> getComponentSetters() {
		return from(propertyMetas).transform(GET_SETTER).toList();
	}

    @Override
	public String toString() {
		return Objects.toStringHelper(this.getClass()).add("propertyMetas", propertyMetas).toString();

	}
}
