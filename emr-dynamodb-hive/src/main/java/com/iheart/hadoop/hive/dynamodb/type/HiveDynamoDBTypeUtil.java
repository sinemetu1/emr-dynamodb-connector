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
import org.apache.hadoop.hive.serde2.lazy.LazyDouble;

import java.util.HashMap;
import java.util.Map;

public class HiveDynamoDBTypeUtil {

  public static AttributeValue parseObject(Object o) {
    if (o instanceof String) {
      return parseString(o);
    } else if (o instanceof LazyDouble) {
      return new AttributeValue().withN(o.toString());
    } else if (o instanceof Map) {
      return parseMap(o);
    } else {
      throw new RuntimeException("Unsupported type: " + o.getClass().getName());
    }
  }

  public static AttributeValue parseString(Object o) {
    String s = (String) o;
    try {
      Double.parseDouble(s);
      return new AttributeValue().withN(s);
    } catch (NumberFormatException ex) {
      return new AttributeValue().withS(s);
    }
  }

  public static AttributeValue parseMap(Map<String, Object> m) {
    Map<String, AttributeValue> toSet = new HashMap<String, AttributeValue>(m.size());
    for (Map.Entry<String, Object> entry : m.entrySet()) {
      String k = entry.getKey();
      toSet.put(k, parseObject(entry.getValue()));
    }
    return new AttributeValue().withM(toSet);
  }

  public static AttributeValue parseMap(Object o) {
    return parseMap((Map<String, Object>) o);
  }

}
