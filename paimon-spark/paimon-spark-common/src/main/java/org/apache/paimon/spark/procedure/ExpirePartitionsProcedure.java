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

package org.apache.paimon.spark.procedure;

import org.apache.paimon.FileStore;
import org.apache.paimon.operation.PartitionExpire;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.utils.Preconditions;
import org.apache.paimon.utils.ProcedureUtils;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;

import java.util.List;
import java.util.Map;

import static org.apache.spark.sql.types.DataTypes.IntegerType;
import static org.apache.spark.sql.types.DataTypes.StringType;

/** A procedure to expire partitions. */
public class ExpirePartitionsProcedure extends BaseProcedure {

    private static final ProcedureParameter[] PARAMETERS =
            new ProcedureParameter[] {
                ProcedureParameter.required("table", StringType),
                ProcedureParameter.optional("expiration_time", StringType),
                ProcedureParameter.optional("timestamp_formatter", StringType),
                ProcedureParameter.optional("timestamp_pattern", StringType),
                ProcedureParameter.optional("expire_strategy", StringType),
                ProcedureParameter.optional("max_expires", IntegerType),
                ProcedureParameter.optional("options", StringType)
            };

    private static final StructType OUTPUT_TYPE =
            new StructType(
                    new StructField[] {
                        new StructField("expired_partitions", StringType, true, Metadata.empty())
                    });

    protected ExpirePartitionsProcedure(TableCatalog tableCatalog) {
        super(tableCatalog);
    }

    @Override
    public ProcedureParameter[] parameters() {
        return PARAMETERS;
    }

    @Override
    public StructType outputType() {
        return OUTPUT_TYPE;
    }

    @Override
    public InternalRow[] call(InternalRow args) {
        Identifier tableIdent = toIdentifier(args.getString(0), PARAMETERS[0].name());
        String expirationTime = args.isNullAt(1) ? null : args.getString(1);
        String timestampFormatter = args.isNullAt(2) ? null : args.getString(2);
        String timestampPattern = args.isNullAt(3) ? null : args.getString(3);
        String expireStrategy = args.isNullAt(4) ? null : args.getString(4);
        Integer maxExpires = args.isNullAt(5) ? null : args.getInt(5);
        String options = args.isNullAt(6) ? null : args.getString(6);

        return modifyPaimonTable(
                tableIdent,
                table -> {
                    Map<String, String> dynamicOptions =
                            ProcedureUtils.fillInPartitionOptions(
                                    expireStrategy,
                                    timestampFormatter,
                                    timestampPattern,
                                    expirationTime,
                                    maxExpires,
                                    options);
                    table = table.copy(dynamicOptions);
                    FileStoreTable fileStoreTable = (FileStoreTable) table;
                    FileStore fileStore = fileStoreTable.store();

                    PartitionExpire partitionExpire =
                            fileStore.newPartitionExpire("", fileStoreTable);
                    Preconditions.checkNotNull(
                            partitionExpire,
                            "Both the partition expiration time and partition field can not be null.");
                    List<Map<String, String>> expired = partitionExpire.expire(Long.MAX_VALUE);
                    return expired == null || expired.isEmpty()
                            ? new InternalRow[] {
                                newInternalRow(UTF8String.fromString("No expired partitions."))
                            }
                            : expired.stream()
                                    .map(
                                            x -> {
                                                String r = x.toString();
                                                return newInternalRow(
                                                        UTF8String.fromString(
                                                                r.substring(1, r.length() - 1)));
                                            })
                                    .toArray(InternalRow[]::new);
                });
    }

    public static ProcedureBuilder builder() {
        return new BaseProcedure.Builder<ExpirePartitionsProcedure>() {
            @Override
            public ExpirePartitionsProcedure doBuild() {
                return new ExpirePartitionsProcedure(tableCatalog());
            }
        };
    }

    @Override
    public String description() {
        return "ExpirePartitionsProcedure";
    }
}
