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

package org.apache.paimon.table;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.TableQueryAuthResult;

/** Checks operations which query authorization does not allow. */
public class QueryAuthChecker {

    private QueryAuthChecker() {}

    /**
     * Fail-closed write gate: a caller whose reads are restricted must not write. A copy-on-write
     * UPDATE or MERGE would persist masked values for everyone; a DELETE drops rows it cannot see.
     * Per caller, so unrestricted writers are unaffected; failing to fetch the rules fails closed.
     */
    public static void checkWrite(FileStoreTable table) {
        CoreOptions options = table.coreOptions();
        if (!options.queryAuthEnabled()) {
            return;
        }
        TableQueryAuthResult result = table.catalogEnvironment().tableQueryAuth(options).auth(null);
        if (result != null && result.hasRules()) {
            throw new UnsupportedOperationException(
                    "Writes to this query-auth table are not supported for this caller: a row "
                            + "filter or column mask applies to its reads, so a DELETE could drop "
                            + "rows it cannot see and an UPDATE or MERGE could write masked "
                            + "values back into the table. Perform the write with an "
                            + "unrestricted principal.");
        }
    }
}
