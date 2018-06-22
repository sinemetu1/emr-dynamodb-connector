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

package com.iheart.hadoop.hive.dynamodb.util;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.hive.dynamodb.util.DynamoDBDataParser;
import org.apache.hadoop.hive.serde2.lazy.LazyDouble;
import org.apache.hadoop.hive.serde2.lazy.LazyMap;
import org.apache.hadoop.hive.serde2.lazy.LazyString;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MyDynamoDBDataParser extends DynamoDBDataParser {

  private static final Log log = LogFactory.getLog(MyDynamoDBDataParser.class);
  public Map<String, Object> getMap(Object data, ObjectInspector objectInspector) {
    if (objectInspector instanceof MapObjectInspector) {
      MapObjectInspector mapOI = ((MapObjectInspector) objectInspector);
      Map<?, ?> aMap = mapOI.getMap(data);
      Map<String, Object> item = new HashMap<String, Object>();
      StringObjectInspector mapKeyObjectInspector = (StringObjectInspector) mapOI
        .getMapKeyObjectInspector();

      // borrowed from HiveDynamoDbItemType
      for (Map.Entry<?,?> entry : aMap.entrySet()) {
        String dynamoDBAttributeName = mapKeyObjectInspector.getPrimitiveJavaObject(entry.getKey());
        Object dynamoDBAttributeValue = entry.getValue();
        item.put(dynamoDBAttributeName, dynamoDBAttributeValue);
      }
      return item;
    } else {
      throw new RuntimeException("Unknown object inspector type: " + objectInspector.getCategory()
        + " Type name: " + objectInspector.getTypeName());
    }
  }

  public List<Object> getListAttribute(Object data, ObjectInspector objectInspector, String
    ddType) {
    ListObjectInspector listObjectInspector = (ListObjectInspector) objectInspector;
    List<?> dataList = listObjectInspector.getList(data);

    if (dataList == null) {
      return null;
    }

    ObjectInspector itemObjectInspector = listObjectInspector.getListElementObjectInspector();
    List<Object> itemList = new ArrayList<Object>();
    // we know hive arrays cannot contain multiple types so we cache the first
    // one and assume all others are the same
    Class listType = null;
    for (Object dataItem : dataList) {
      if (dataItem == null) {
        throw new RuntimeException("Null element found in list: " + dataList);
      }

      log.warn("dataItem class:" + dataItem.getClass().getName());
      if (ddType.equals("L")) {
        if (listType == String.class || dataItem instanceof LazyString) {
          itemList.add(getString(dataItem, itemObjectInspector));
          listType = LazyString.class;
        } if (listType == LazyMap.class || dataItem instanceof LazyMap) {
          itemList.add(getMap(dataItem, itemObjectInspector));
          listType = LazyMap.class;
        } else {
          itemList.add(getNumber(dataItem, itemObjectInspector));
          listType = LazyDouble.class;
        }
      } else {
        throw new RuntimeException("Unsupported dynamodb type: " + ddType +
          " dataItem class: " + dataItem.getClass().getName());
      }
    }

    return itemList;
  }
}
