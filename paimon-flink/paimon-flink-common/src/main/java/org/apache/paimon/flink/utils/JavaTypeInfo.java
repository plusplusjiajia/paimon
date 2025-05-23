/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.flink.utils;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.serialization.SerializerConfig;
import org.apache.flink.api.common.typeinfo.AtomicType;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.runtime.GenericTypeComparator;

import java.io.Serializable;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** A {@link TypeInformation} which uses java serialization. */
public class JavaTypeInfo<T extends Serializable> extends TypeInformation<T>
        implements AtomicType<T> {

    private static final long serialVersionUID = 1L;

    private final Class<T> typeClass;

    public JavaTypeInfo(Class<T> typeClass) {
        this.typeClass = checkNotNull(typeClass);
    }

    @Override
    @PublicEvolving
    public boolean isBasicType() {
        return false;
    }

    @Override
    @PublicEvolving
    public boolean isTupleType() {
        return false;
    }

    @Override
    @PublicEvolving
    public int getArity() {
        return 1;
    }

    @Override
    @PublicEvolving
    public int getTotalFields() {
        return 1;
    }

    @Override
    @PublicEvolving
    public Class<T> getTypeClass() {
        return typeClass;
    }

    @Override
    @PublicEvolving
    public boolean isKeyType() {
        return Comparable.class.isAssignableFrom(typeClass);
    }

    /**
     * Do not annotate with <code>@override</code> here to maintain compatibility with Flink 1.18-.
     */
    public TypeSerializer<T> createSerializer(SerializerConfig config) {
        return this.createSerializer((ExecutionConfig) null);
    }

    /**
     * Do not annotate with <code>@override</code> here to maintain compatibility with Flink 2.0+.
     */
    public TypeSerializer<T> createSerializer(ExecutionConfig config) {
        return new JavaSerializer<>(this.typeClass);
    }

    @SuppressWarnings("unchecked")
    @Override
    public TypeComparator<T> createComparator(
            boolean sortOrderAscending, ExecutionConfig executionConfig) {
        if (isKeyType()) {
            @SuppressWarnings("rawtypes")
            GenericTypeComparator comparator =
                    new GenericTypeComparator(
                            sortOrderAscending,
                            new JavaSerializer<>(this.typeClass),
                            this.typeClass);
            return (TypeComparator<T>) comparator;
        }

        throw new UnsupportedOperationException(
                "Types that do not implement java.lang.Comparable cannot be used as keys.");
    }

    @Override
    public int hashCode() {
        return typeClass.hashCode();
    }

    @Override
    public boolean canEqual(Object obj) {
        return obj instanceof JavaTypeInfo;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof JavaTypeInfo) {
            @SuppressWarnings("unchecked")
            JavaTypeInfo<T> genericTypeInfo = (JavaTypeInfo<T>) obj;

            return typeClass == genericTypeInfo.typeClass;
        } else {
            return false;
        }
    }

    @Override
    public String toString() {
        return "JavaType<" + typeClass.getCanonicalName() + ">";
    }
}
