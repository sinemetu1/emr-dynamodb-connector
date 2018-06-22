/**
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

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.iheart.hadoop.dynamodb.type.DynamoDBListType;
import com.iheart.hadoop.hive.dynamodb.util.MyDynamoDBDataParser;
import org.apache.hadoop.hive.dynamodb.type.HiveDynamoDBType;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;

import java.util.ArrayList;
import java.util.List;

import static com.iheart.hadoop.hive.dynamodb.type.HiveDynamoDBTypeUtil.parseObject;

public class HiveDynamoDBListType extends DynamoDBListType implements HiveDynamoDBType {

  private final MyDynamoDBDataParser parser = new MyDynamoDBDataParser();

  @Override
  public AttributeValue getDynamoDBData(Object data, ObjectInspector objectInspector) {
    List<Object> values = parser.getListAttribute(data, objectInspector, getDynamoDBType());
    if ((values != null) && (!values.isEmpty())) {
      List<AttributeValue> toSet = new ArrayList<AttributeValue>();
      AttributeValue outer = new AttributeValue();
      for (Object v : values) {
        toSet.add(parseObject(v));
      }
      return outer.withL(toSet);
    } else {
      return null;
    }
  }

  @Override
  public Object getHiveData(AttributeValue data, String hiveType) {
    if (data == null) {
      return null;
    }
    return data.getL();
  }

}
