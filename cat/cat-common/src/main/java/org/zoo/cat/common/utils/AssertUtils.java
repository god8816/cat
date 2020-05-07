/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.zoo.cat.common.utils;

import org.zoo.cat.common.exception.CatRuntimeException;

/**
 * AssertUtils.
 *
 * @author dzc
 */
public final class AssertUtils {

    private AssertUtils() {

    }

    /**
     * Not null.
     *
     * @param obj the obj
     */
    public static void notNull(final Object obj) {
        if (obj == null) {
            throw new CatRuntimeException("argument invalid,Please check");
        }
    }

}
