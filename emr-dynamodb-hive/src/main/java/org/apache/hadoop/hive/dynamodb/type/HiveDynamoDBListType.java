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

package org.apache.hadoop.hive.dynamodb.type;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import org.apache.hadoop.dynamodb.type.DynamoDBListType;
import org.apache.hadoop.hive.dynamodb.util.DynamoDBDataParser;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HiveDynamoDBListType extends DynamoDBListType implements HiveDynamoDBType {

  private final DynamoDBDataParser parser = new DynamoDBDataParser();

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

  private AttributeValue parseObject(Object o) {
    if (o instanceof String) {
      return parseString(o);
    } else if (o instanceof Map) {
      return parseMap(o);
    } else {
      throw new RuntimeException("Unsupported type: " + o.getClass().getName());
    }
  }

  private AttributeValue parseString(Object o) {
    String s = (String) o;
    try {
      Double.parseDouble(s);
      return new AttributeValue().withN(s);
    } catch (NumberFormatException ex) {
      return new AttributeValue().withS(s);
    }
  }

  private AttributeValue parseMap(Object o) {
    Map<String, Object> m = (Map<String, Object>) o;
    Map<String, AttributeValue> toSet = new HashMap<String, AttributeValue>(m.size());
    for (Map.Entry entry : m.entrySet()) {
      String k = (String) entry.getKey();
      toSet.put(k, parseObject(entry.getValue()));
    }
    return new AttributeValue().withM(toSet);
  }

  @Override
  public Object getHiveData(AttributeValue data, String hiveType) {
    if (data == null) {
      return null;
    }
    return data.getL();
  }

}
