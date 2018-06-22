/**
 *
 * Licensed under the Apache License, Version 2.0 (the "License"). You may not use this file
 * except in compliance with the License. A copy of the License is located at
 *
 *     http://aws.amazon.com/apache2.0/
 *
 * or in the "LICENSE.TXT" file accompanying this file. This file is distributed on an "AS IS"
 * BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under the License.
 */

package com.iheart.hadoop.hive.dynamodb.type;

import org.apache.hadoop.hive.dynamodb.type.HiveDynamoDBTypeFactory;

import org.apache.hadoop.hive.dynamodb.DerivedHiveTypeConstants;
import org.apache.hadoop.hive.dynamodb.type.*;
import org.apache.hadoop.hive.serde.serdeConstants;

import java.util.HashMap;
import java.util.Map;

public class MyHiveDynamoDBTypeFactory extends HiveDynamoDBTypeFactory {

  private static final HiveDynamoDBType STRING_TYPE = new HiveDynamoDBStringType();
  private static final HiveDynamoDBType NUMBER_TYPE = new HiveDynamoDBNumberType();
  private static final HiveDynamoDBType BINARY_TYPE = new HiveDynamoDBBinaryType();
  private static final HiveDynamoDBType BOOLEAN_TYPE = new HiveDynamoDBBooleanType();


  private static final HiveDynamoDBType NUMBER_SET_TYPE = new HiveDynamoDBNumberSetType();
  private static final HiveDynamoDBType STRING_SET_TYPE = new HiveDynamoDBStringSetType();
  private static final HiveDynamoDBType BINARY_SET_TYPE = new HiveDynamoDBBinarySetType();


  private static final HiveDynamoDBType NUMBER_LIST_TYPE = new HiveDynamoDBListType();
  private static final HiveDynamoDBType STRING_LIST_TYPE = new HiveDynamoDBListType();
  private static final HiveDynamoDBType LIST_ITEM_TYPE = new HiveDynamoDBListType();
  private static final HiveDynamoDBType MAP_TYPE = new HiveDynamoDBMapType();

  private static final Map<String, HiveDynamoDBType> HIVE_TYPE_MAP = new HashMap<>();

  static {
    HIVE_TYPE_MAP.put(serdeConstants.STRING_TYPE_NAME, STRING_TYPE);
    HIVE_TYPE_MAP.put(serdeConstants.DOUBLE_TYPE_NAME, NUMBER_TYPE);
    HIVE_TYPE_MAP.put(serdeConstants.BIGINT_TYPE_NAME, NUMBER_TYPE);
    HIVE_TYPE_MAP.put(serdeConstants.BINARY_TYPE_NAME, BINARY_TYPE);
    HIVE_TYPE_MAP.put(serdeConstants.BOOLEAN_TYPE_NAME, BOOLEAN_TYPE);


//    HIVE_TYPE_MAP.put(DerivedHiveTypeConstants.BIGINT_ARRAY_TYPE_NAME, NUMBER_SET_TYPE);
//    HIVE_TYPE_MAP.put(DerivedHiveTypeConstants.DOUBLE_ARRAY_TYPE_NAME, NUMBER_SET_TYPE);
//    HIVE_TYPE_MAP.put(DerivedHiveTypeConstants.STRING_ARRAY_TYPE_NAME, STRING_SET_TYPE);
    HIVE_TYPE_MAP.put(DerivedHiveTypeConstants.BINARY_ARRAY_TYPE_NAME, BINARY_SET_TYPE);

    HIVE_TYPE_MAP.put(DerivedHiveTypeConstants.BIGINT_ARRAY_LIST_TYPE_NAME, NUMBER_LIST_TYPE);
    HIVE_TYPE_MAP.put(DerivedHiveTypeConstants.DOUBLE_ARRAY_LIST_TYPE_NAME, NUMBER_LIST_TYPE);
    HIVE_TYPE_MAP.put(DerivedHiveTypeConstants.STRING_ARRAY_LIST_TYPE_NAME, STRING_LIST_TYPE);
    HIVE_TYPE_MAP.put(DerivedHiveTypeConstants.LIST_ITEM_MAP_TYPE_NAME, LIST_ITEM_TYPE);
    HIVE_TYPE_MAP.put(DerivedHiveTypeConstants.STRING_BIGINT_MAP_TYPE_NAME, MAP_TYPE);
    HIVE_TYPE_MAP.put(DerivedHiveTypeConstants.LIST_STRING_BIG_INT_MAP_TYPE_NAME, LIST_ITEM_TYPE);
    HIVE_TYPE_MAP.put(DerivedHiveTypeConstants.LIST_STRING_BIG_DOUBLE_MAP_TYPE_NAME, LIST_ITEM_TYPE);

    HIVE_TYPE_MAP.put(DerivedHiveTypeConstants.ITEM_MAP_TYPE_NAME, DYNAMODB_ITEM_TYPE);
  }
}
