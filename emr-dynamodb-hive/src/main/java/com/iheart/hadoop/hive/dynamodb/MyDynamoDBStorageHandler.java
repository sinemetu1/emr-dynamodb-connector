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

package com.iheart.hadoop.hive.dynamodb;

import com.iheart.hadoop.hive.dynamodb.type.MyHiveDynamoDBTypeFactory;
import org.apache.hadoop.hive.dynamodb.DynamoDBStorageHandler;
import org.apache.hadoop.hive.dynamodb.type.HiveDynamoDBType;

public class MyDynamoDBStorageHandler extends DynamoDBStorageHandler {
  @Override
  protected boolean isHiveDynamoDBItemMapType(String type){
    return MyHiveDynamoDBTypeFactory.isHiveDynamoDBItemMapType(type);
  }

  @Override
  protected HiveDynamoDBType getTypeObjectFromHiveType(String type){
    return MyHiveDynamoDBTypeFactory.getTypeObjectFromHiveType(type);
  }
}
