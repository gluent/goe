// ORM class for table 'SH_TEST.STORY_NOSSH'
// WARNING: This class is AUTO-GENERATED. Modify at your own risk.
//
// Debug information:
// Generated date: Wed Mar 18 08:44:53 UTC 2020
// For connector: org.apache.sqoop.manager.oracle.OraOopConnManager
package GluentOffload.nossh_sh_test;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.lib.db.DBWritable;
import com.cloudera.sqoop.lib.JdbcWritableBridge;
import com.cloudera.sqoop.lib.DelimiterSet;
import com.cloudera.sqoop.lib.FieldFormatter;
import com.cloudera.sqoop.lib.RecordParser;
import com.cloudera.sqoop.lib.BooleanParser;
import com.cloudera.sqoop.lib.BlobRef;
import com.cloudera.sqoop.lib.ClobRef;
import com.cloudera.sqoop.lib.LargeObjectLoader;
import com.cloudera.sqoop.lib.SqoopRecord;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.HashMap;

public class story_nossh extends SqoopRecord  implements DBWritable, Writable {
  private final int PROTOCOL_VERSION = 3;
  public int getClassFormatVersion() { return PROTOCOL_VERSION; }
  public static interface FieldSetterCommand {    void setField(Object value);  }  protected ResultSet __cur_result_set;
  private Map<String, FieldSetterCommand> setters = new HashMap<String, FieldSetterCommand>();
  private void init0() {
    setters.put("PROD_ID", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        PROD_ID = (Integer)value;
      }
    });
    setters.put("PROD_NAME", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        PROD_NAME = (String)value;
      }
    });
    setters.put("PROD_DESC", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        PROD_DESC = (String)value;
      }
    });
    setters.put("PROD_SUBCATEGORY", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        PROD_SUBCATEGORY = (String)value;
      }
    });
    setters.put("PROD_SUBCATEGORY_ID", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        PROD_SUBCATEGORY_ID = (String)value;
      }
    });
    setters.put("PROD_SUBCATEGORY_DESC", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        PROD_SUBCATEGORY_DESC = (String)value;
      }
    });
    setters.put("PROD_CATEGORY", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        PROD_CATEGORY = (String)value;
      }
    });
    setters.put("PROD_CATEGORY_ID", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        PROD_CATEGORY_ID = (String)value;
      }
    });
    setters.put("PROD_CATEGORY_DESC", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        PROD_CATEGORY_DESC = (String)value;
      }
    });
    setters.put("PROD_WEIGHT_CLASS", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        PROD_WEIGHT_CLASS = (Integer)value;
      }
    });
    setters.put("PROD_UNIT_OF_MEASURE", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        PROD_UNIT_OF_MEASURE = (String)value;
      }
    });
    setters.put("PROD_PACK_SIZE", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        PROD_PACK_SIZE = (String)value;
      }
    });
    setters.put("SUPPLIER_ID", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        SUPPLIER_ID = (Integer)value;
      }
    });
    setters.put("PROD_STATUS", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        PROD_STATUS = (String)value;
      }
    });
    setters.put("PROD_LIST_PRICE", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        PROD_LIST_PRICE = (String)value;
      }
    });
    setters.put("PROD_MIN_PRICE", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        PROD_MIN_PRICE = (String)value;
      }
    });
    setters.put("PROD_TOTAL", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        PROD_TOTAL = (String)value;
      }
    });
    setters.put("PROD_TOTAL_ID", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        PROD_TOTAL_ID = (String)value;
      }
    });
    setters.put("PROD_SRC_ID", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        PROD_SRC_ID = (String)value;
      }
    });
    setters.put("PROD_EFF_FROM", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        PROD_EFF_FROM = (String)value;
      }
    });
    setters.put("PROD_EFF_TO", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        PROD_EFF_TO = (String)value;
      }
    });
    setters.put("PROD_VALID", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        PROD_VALID = (String)value;
      }
    });
  }
  public story_nossh() {
    init0();
  }
  private Integer PROD_ID;
  public Integer get_PROD_ID() {
    return PROD_ID;
  }
  public void set_PROD_ID(Integer PROD_ID) {
    this.PROD_ID = PROD_ID;
  }
  public story_nossh with_PROD_ID(Integer PROD_ID) {
    this.PROD_ID = PROD_ID;
    return this;
  }
  private String PROD_NAME;
  public String get_PROD_NAME() {
    return PROD_NAME;
  }
  public void set_PROD_NAME(String PROD_NAME) {
    this.PROD_NAME = PROD_NAME;
  }
  public story_nossh with_PROD_NAME(String PROD_NAME) {
    this.PROD_NAME = PROD_NAME;
    return this;
  }
  private String PROD_DESC;
  public String get_PROD_DESC() {
    return PROD_DESC;
  }
  public void set_PROD_DESC(String PROD_DESC) {
    this.PROD_DESC = PROD_DESC;
  }
  public story_nossh with_PROD_DESC(String PROD_DESC) {
    this.PROD_DESC = PROD_DESC;
    return this;
  }
  private String PROD_SUBCATEGORY;
  public String get_PROD_SUBCATEGORY() {
    return PROD_SUBCATEGORY;
  }
  public void set_PROD_SUBCATEGORY(String PROD_SUBCATEGORY) {
    this.PROD_SUBCATEGORY = PROD_SUBCATEGORY;
  }
  public story_nossh with_PROD_SUBCATEGORY(String PROD_SUBCATEGORY) {
    this.PROD_SUBCATEGORY = PROD_SUBCATEGORY;
    return this;
  }
  private String PROD_SUBCATEGORY_ID;
  public String get_PROD_SUBCATEGORY_ID() {
    return PROD_SUBCATEGORY_ID;
  }
  public void set_PROD_SUBCATEGORY_ID(String PROD_SUBCATEGORY_ID) {
    this.PROD_SUBCATEGORY_ID = PROD_SUBCATEGORY_ID;
  }
  public story_nossh with_PROD_SUBCATEGORY_ID(String PROD_SUBCATEGORY_ID) {
    this.PROD_SUBCATEGORY_ID = PROD_SUBCATEGORY_ID;
    return this;
  }
  private String PROD_SUBCATEGORY_DESC;
  public String get_PROD_SUBCATEGORY_DESC() {
    return PROD_SUBCATEGORY_DESC;
  }
  public void set_PROD_SUBCATEGORY_DESC(String PROD_SUBCATEGORY_DESC) {
    this.PROD_SUBCATEGORY_DESC = PROD_SUBCATEGORY_DESC;
  }
  public story_nossh with_PROD_SUBCATEGORY_DESC(String PROD_SUBCATEGORY_DESC) {
    this.PROD_SUBCATEGORY_DESC = PROD_SUBCATEGORY_DESC;
    return this;
  }
  private String PROD_CATEGORY;
  public String get_PROD_CATEGORY() {
    return PROD_CATEGORY;
  }
  public void set_PROD_CATEGORY(String PROD_CATEGORY) {
    this.PROD_CATEGORY = PROD_CATEGORY;
  }
  public story_nossh with_PROD_CATEGORY(String PROD_CATEGORY) {
    this.PROD_CATEGORY = PROD_CATEGORY;
    return this;
  }
  private String PROD_CATEGORY_ID;
  public String get_PROD_CATEGORY_ID() {
    return PROD_CATEGORY_ID;
  }
  public void set_PROD_CATEGORY_ID(String PROD_CATEGORY_ID) {
    this.PROD_CATEGORY_ID = PROD_CATEGORY_ID;
  }
  public story_nossh with_PROD_CATEGORY_ID(String PROD_CATEGORY_ID) {
    this.PROD_CATEGORY_ID = PROD_CATEGORY_ID;
    return this;
  }
  private String PROD_CATEGORY_DESC;
  public String get_PROD_CATEGORY_DESC() {
    return PROD_CATEGORY_DESC;
  }
  public void set_PROD_CATEGORY_DESC(String PROD_CATEGORY_DESC) {
    this.PROD_CATEGORY_DESC = PROD_CATEGORY_DESC;
  }
  public story_nossh with_PROD_CATEGORY_DESC(String PROD_CATEGORY_DESC) {
    this.PROD_CATEGORY_DESC = PROD_CATEGORY_DESC;
    return this;
  }
  private Integer PROD_WEIGHT_CLASS;
  public Integer get_PROD_WEIGHT_CLASS() {
    return PROD_WEIGHT_CLASS;
  }
  public void set_PROD_WEIGHT_CLASS(Integer PROD_WEIGHT_CLASS) {
    this.PROD_WEIGHT_CLASS = PROD_WEIGHT_CLASS;
  }
  public story_nossh with_PROD_WEIGHT_CLASS(Integer PROD_WEIGHT_CLASS) {
    this.PROD_WEIGHT_CLASS = PROD_WEIGHT_CLASS;
    return this;
  }
  private String PROD_UNIT_OF_MEASURE;
  public String get_PROD_UNIT_OF_MEASURE() {
    return PROD_UNIT_OF_MEASURE;
  }
  public void set_PROD_UNIT_OF_MEASURE(String PROD_UNIT_OF_MEASURE) {
    this.PROD_UNIT_OF_MEASURE = PROD_UNIT_OF_MEASURE;
  }
  public story_nossh with_PROD_UNIT_OF_MEASURE(String PROD_UNIT_OF_MEASURE) {
    this.PROD_UNIT_OF_MEASURE = PROD_UNIT_OF_MEASURE;
    return this;
  }
  private String PROD_PACK_SIZE;
  public String get_PROD_PACK_SIZE() {
    return PROD_PACK_SIZE;
  }
  public void set_PROD_PACK_SIZE(String PROD_PACK_SIZE) {
    this.PROD_PACK_SIZE = PROD_PACK_SIZE;
  }
  public story_nossh with_PROD_PACK_SIZE(String PROD_PACK_SIZE) {
    this.PROD_PACK_SIZE = PROD_PACK_SIZE;
    return this;
  }
  private Integer SUPPLIER_ID;
  public Integer get_SUPPLIER_ID() {
    return SUPPLIER_ID;
  }
  public void set_SUPPLIER_ID(Integer SUPPLIER_ID) {
    this.SUPPLIER_ID = SUPPLIER_ID;
  }
  public story_nossh with_SUPPLIER_ID(Integer SUPPLIER_ID) {
    this.SUPPLIER_ID = SUPPLIER_ID;
    return this;
  }
  private String PROD_STATUS;
  public String get_PROD_STATUS() {
    return PROD_STATUS;
  }
  public void set_PROD_STATUS(String PROD_STATUS) {
    this.PROD_STATUS = PROD_STATUS;
  }
  public story_nossh with_PROD_STATUS(String PROD_STATUS) {
    this.PROD_STATUS = PROD_STATUS;
    return this;
  }
  private String PROD_LIST_PRICE;
  public String get_PROD_LIST_PRICE() {
    return PROD_LIST_PRICE;
  }
  public void set_PROD_LIST_PRICE(String PROD_LIST_PRICE) {
    this.PROD_LIST_PRICE = PROD_LIST_PRICE;
  }
  public story_nossh with_PROD_LIST_PRICE(String PROD_LIST_PRICE) {
    this.PROD_LIST_PRICE = PROD_LIST_PRICE;
    return this;
  }
  private String PROD_MIN_PRICE;
  public String get_PROD_MIN_PRICE() {
    return PROD_MIN_PRICE;
  }
  public void set_PROD_MIN_PRICE(String PROD_MIN_PRICE) {
    this.PROD_MIN_PRICE = PROD_MIN_PRICE;
  }
  public story_nossh with_PROD_MIN_PRICE(String PROD_MIN_PRICE) {
    this.PROD_MIN_PRICE = PROD_MIN_PRICE;
    return this;
  }
  private String PROD_TOTAL;
  public String get_PROD_TOTAL() {
    return PROD_TOTAL;
  }
  public void set_PROD_TOTAL(String PROD_TOTAL) {
    this.PROD_TOTAL = PROD_TOTAL;
  }
  public story_nossh with_PROD_TOTAL(String PROD_TOTAL) {
    this.PROD_TOTAL = PROD_TOTAL;
    return this;
  }
  private String PROD_TOTAL_ID;
  public String get_PROD_TOTAL_ID() {
    return PROD_TOTAL_ID;
  }
  public void set_PROD_TOTAL_ID(String PROD_TOTAL_ID) {
    this.PROD_TOTAL_ID = PROD_TOTAL_ID;
  }
  public story_nossh with_PROD_TOTAL_ID(String PROD_TOTAL_ID) {
    this.PROD_TOTAL_ID = PROD_TOTAL_ID;
    return this;
  }
  private String PROD_SRC_ID;
  public String get_PROD_SRC_ID() {
    return PROD_SRC_ID;
  }
  public void set_PROD_SRC_ID(String PROD_SRC_ID) {
    this.PROD_SRC_ID = PROD_SRC_ID;
  }
  public story_nossh with_PROD_SRC_ID(String PROD_SRC_ID) {
    this.PROD_SRC_ID = PROD_SRC_ID;
    return this;
  }
  private String PROD_EFF_FROM;
  public String get_PROD_EFF_FROM() {
    return PROD_EFF_FROM;
  }
  public void set_PROD_EFF_FROM(String PROD_EFF_FROM) {
    this.PROD_EFF_FROM = PROD_EFF_FROM;
  }
  public story_nossh with_PROD_EFF_FROM(String PROD_EFF_FROM) {
    this.PROD_EFF_FROM = PROD_EFF_FROM;
    return this;
  }
  private String PROD_EFF_TO;
  public String get_PROD_EFF_TO() {
    return PROD_EFF_TO;
  }
  public void set_PROD_EFF_TO(String PROD_EFF_TO) {
    this.PROD_EFF_TO = PROD_EFF_TO;
  }
  public story_nossh with_PROD_EFF_TO(String PROD_EFF_TO) {
    this.PROD_EFF_TO = PROD_EFF_TO;
    return this;
  }
  private String PROD_VALID;
  public String get_PROD_VALID() {
    return PROD_VALID;
  }
  public void set_PROD_VALID(String PROD_VALID) {
    this.PROD_VALID = PROD_VALID;
  }
  public story_nossh with_PROD_VALID(String PROD_VALID) {
    this.PROD_VALID = PROD_VALID;
    return this;
  }
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof story_nossh)) {
      return false;
    }
    story_nossh that = (story_nossh) o;
    boolean equal = true;
    equal = equal && (this.PROD_ID == null ? that.PROD_ID == null : this.PROD_ID.equals(that.PROD_ID));
    equal = equal && (this.PROD_NAME == null ? that.PROD_NAME == null : this.PROD_NAME.equals(that.PROD_NAME));
    equal = equal && (this.PROD_DESC == null ? that.PROD_DESC == null : this.PROD_DESC.equals(that.PROD_DESC));
    equal = equal && (this.PROD_SUBCATEGORY == null ? that.PROD_SUBCATEGORY == null : this.PROD_SUBCATEGORY.equals(that.PROD_SUBCATEGORY));
    equal = equal && (this.PROD_SUBCATEGORY_ID == null ? that.PROD_SUBCATEGORY_ID == null : this.PROD_SUBCATEGORY_ID.equals(that.PROD_SUBCATEGORY_ID));
    equal = equal && (this.PROD_SUBCATEGORY_DESC == null ? that.PROD_SUBCATEGORY_DESC == null : this.PROD_SUBCATEGORY_DESC.equals(that.PROD_SUBCATEGORY_DESC));
    equal = equal && (this.PROD_CATEGORY == null ? that.PROD_CATEGORY == null : this.PROD_CATEGORY.equals(that.PROD_CATEGORY));
    equal = equal && (this.PROD_CATEGORY_ID == null ? that.PROD_CATEGORY_ID == null : this.PROD_CATEGORY_ID.equals(that.PROD_CATEGORY_ID));
    equal = equal && (this.PROD_CATEGORY_DESC == null ? that.PROD_CATEGORY_DESC == null : this.PROD_CATEGORY_DESC.equals(that.PROD_CATEGORY_DESC));
    equal = equal && (this.PROD_WEIGHT_CLASS == null ? that.PROD_WEIGHT_CLASS == null : this.PROD_WEIGHT_CLASS.equals(that.PROD_WEIGHT_CLASS));
    equal = equal && (this.PROD_UNIT_OF_MEASURE == null ? that.PROD_UNIT_OF_MEASURE == null : this.PROD_UNIT_OF_MEASURE.equals(that.PROD_UNIT_OF_MEASURE));
    equal = equal && (this.PROD_PACK_SIZE == null ? that.PROD_PACK_SIZE == null : this.PROD_PACK_SIZE.equals(that.PROD_PACK_SIZE));
    equal = equal && (this.SUPPLIER_ID == null ? that.SUPPLIER_ID == null : this.SUPPLIER_ID.equals(that.SUPPLIER_ID));
    equal = equal && (this.PROD_STATUS == null ? that.PROD_STATUS == null : this.PROD_STATUS.equals(that.PROD_STATUS));
    equal = equal && (this.PROD_LIST_PRICE == null ? that.PROD_LIST_PRICE == null : this.PROD_LIST_PRICE.equals(that.PROD_LIST_PRICE));
    equal = equal && (this.PROD_MIN_PRICE == null ? that.PROD_MIN_PRICE == null : this.PROD_MIN_PRICE.equals(that.PROD_MIN_PRICE));
    equal = equal && (this.PROD_TOTAL == null ? that.PROD_TOTAL == null : this.PROD_TOTAL.equals(that.PROD_TOTAL));
    equal = equal && (this.PROD_TOTAL_ID == null ? that.PROD_TOTAL_ID == null : this.PROD_TOTAL_ID.equals(that.PROD_TOTAL_ID));
    equal = equal && (this.PROD_SRC_ID == null ? that.PROD_SRC_ID == null : this.PROD_SRC_ID.equals(that.PROD_SRC_ID));
    equal = equal && (this.PROD_EFF_FROM == null ? that.PROD_EFF_FROM == null : this.PROD_EFF_FROM.equals(that.PROD_EFF_FROM));
    equal = equal && (this.PROD_EFF_TO == null ? that.PROD_EFF_TO == null : this.PROD_EFF_TO.equals(that.PROD_EFF_TO));
    equal = equal && (this.PROD_VALID == null ? that.PROD_VALID == null : this.PROD_VALID.equals(that.PROD_VALID));
    return equal;
  }
  public boolean equals0(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof story_nossh)) {
      return false;
    }
    story_nossh that = (story_nossh) o;
    boolean equal = true;
    equal = equal && (this.PROD_ID == null ? that.PROD_ID == null : this.PROD_ID.equals(that.PROD_ID));
    equal = equal && (this.PROD_NAME == null ? that.PROD_NAME == null : this.PROD_NAME.equals(that.PROD_NAME));
    equal = equal && (this.PROD_DESC == null ? that.PROD_DESC == null : this.PROD_DESC.equals(that.PROD_DESC));
    equal = equal && (this.PROD_SUBCATEGORY == null ? that.PROD_SUBCATEGORY == null : this.PROD_SUBCATEGORY.equals(that.PROD_SUBCATEGORY));
    equal = equal && (this.PROD_SUBCATEGORY_ID == null ? that.PROD_SUBCATEGORY_ID == null : this.PROD_SUBCATEGORY_ID.equals(that.PROD_SUBCATEGORY_ID));
    equal = equal && (this.PROD_SUBCATEGORY_DESC == null ? that.PROD_SUBCATEGORY_DESC == null : this.PROD_SUBCATEGORY_DESC.equals(that.PROD_SUBCATEGORY_DESC));
    equal = equal && (this.PROD_CATEGORY == null ? that.PROD_CATEGORY == null : this.PROD_CATEGORY.equals(that.PROD_CATEGORY));
    equal = equal && (this.PROD_CATEGORY_ID == null ? that.PROD_CATEGORY_ID == null : this.PROD_CATEGORY_ID.equals(that.PROD_CATEGORY_ID));
    equal = equal && (this.PROD_CATEGORY_DESC == null ? that.PROD_CATEGORY_DESC == null : this.PROD_CATEGORY_DESC.equals(that.PROD_CATEGORY_DESC));
    equal = equal && (this.PROD_WEIGHT_CLASS == null ? that.PROD_WEIGHT_CLASS == null : this.PROD_WEIGHT_CLASS.equals(that.PROD_WEIGHT_CLASS));
    equal = equal && (this.PROD_UNIT_OF_MEASURE == null ? that.PROD_UNIT_OF_MEASURE == null : this.PROD_UNIT_OF_MEASURE.equals(that.PROD_UNIT_OF_MEASURE));
    equal = equal && (this.PROD_PACK_SIZE == null ? that.PROD_PACK_SIZE == null : this.PROD_PACK_SIZE.equals(that.PROD_PACK_SIZE));
    equal = equal && (this.SUPPLIER_ID == null ? that.SUPPLIER_ID == null : this.SUPPLIER_ID.equals(that.SUPPLIER_ID));
    equal = equal && (this.PROD_STATUS == null ? that.PROD_STATUS == null : this.PROD_STATUS.equals(that.PROD_STATUS));
    equal = equal && (this.PROD_LIST_PRICE == null ? that.PROD_LIST_PRICE == null : this.PROD_LIST_PRICE.equals(that.PROD_LIST_PRICE));
    equal = equal && (this.PROD_MIN_PRICE == null ? that.PROD_MIN_PRICE == null : this.PROD_MIN_PRICE.equals(that.PROD_MIN_PRICE));
    equal = equal && (this.PROD_TOTAL == null ? that.PROD_TOTAL == null : this.PROD_TOTAL.equals(that.PROD_TOTAL));
    equal = equal && (this.PROD_TOTAL_ID == null ? that.PROD_TOTAL_ID == null : this.PROD_TOTAL_ID.equals(that.PROD_TOTAL_ID));
    equal = equal && (this.PROD_SRC_ID == null ? that.PROD_SRC_ID == null : this.PROD_SRC_ID.equals(that.PROD_SRC_ID));
    equal = equal && (this.PROD_EFF_FROM == null ? that.PROD_EFF_FROM == null : this.PROD_EFF_FROM.equals(that.PROD_EFF_FROM));
    equal = equal && (this.PROD_EFF_TO == null ? that.PROD_EFF_TO == null : this.PROD_EFF_TO.equals(that.PROD_EFF_TO));
    equal = equal && (this.PROD_VALID == null ? that.PROD_VALID == null : this.PROD_VALID.equals(that.PROD_VALID));
    return equal;
  }
  public void readFields(ResultSet __dbResults) throws SQLException {
    this.__cur_result_set = __dbResults;
    this.PROD_ID = JdbcWritableBridge.readInteger(1, __dbResults);
    this.PROD_NAME = JdbcWritableBridge.readString(2, __dbResults);
    this.PROD_DESC = JdbcWritableBridge.readString(3, __dbResults);
    this.PROD_SUBCATEGORY = JdbcWritableBridge.readString(4, __dbResults);
    this.PROD_SUBCATEGORY_ID = JdbcWritableBridge.readString(5, __dbResults);
    this.PROD_SUBCATEGORY_DESC = JdbcWritableBridge.readString(6, __dbResults);
    this.PROD_CATEGORY = JdbcWritableBridge.readString(7, __dbResults);
    this.PROD_CATEGORY_ID = JdbcWritableBridge.readString(8, __dbResults);
    this.PROD_CATEGORY_DESC = JdbcWritableBridge.readString(9, __dbResults);
    this.PROD_WEIGHT_CLASS = JdbcWritableBridge.readInteger(10, __dbResults);
    this.PROD_UNIT_OF_MEASURE = JdbcWritableBridge.readString(11, __dbResults);
    this.PROD_PACK_SIZE = JdbcWritableBridge.readString(12, __dbResults);
    this.SUPPLIER_ID = JdbcWritableBridge.readInteger(13, __dbResults);
    this.PROD_STATUS = JdbcWritableBridge.readString(14, __dbResults);
    this.PROD_LIST_PRICE = JdbcWritableBridge.readString(15, __dbResults);
    this.PROD_MIN_PRICE = JdbcWritableBridge.readString(16, __dbResults);
    this.PROD_TOTAL = JdbcWritableBridge.readString(17, __dbResults);
    this.PROD_TOTAL_ID = JdbcWritableBridge.readString(18, __dbResults);
    this.PROD_SRC_ID = JdbcWritableBridge.readString(19, __dbResults);
    this.PROD_EFF_FROM = JdbcWritableBridge.readString(20, __dbResults);
    this.PROD_EFF_TO = JdbcWritableBridge.readString(21, __dbResults);
    this.PROD_VALID = JdbcWritableBridge.readString(22, __dbResults);
  }
  public void readFields0(ResultSet __dbResults) throws SQLException {
    this.PROD_ID = JdbcWritableBridge.readInteger(1, __dbResults);
    this.PROD_NAME = JdbcWritableBridge.readString(2, __dbResults);
    this.PROD_DESC = JdbcWritableBridge.readString(3, __dbResults);
    this.PROD_SUBCATEGORY = JdbcWritableBridge.readString(4, __dbResults);
    this.PROD_SUBCATEGORY_ID = JdbcWritableBridge.readString(5, __dbResults);
    this.PROD_SUBCATEGORY_DESC = JdbcWritableBridge.readString(6, __dbResults);
    this.PROD_CATEGORY = JdbcWritableBridge.readString(7, __dbResults);
    this.PROD_CATEGORY_ID = JdbcWritableBridge.readString(8, __dbResults);
    this.PROD_CATEGORY_DESC = JdbcWritableBridge.readString(9, __dbResults);
    this.PROD_WEIGHT_CLASS = JdbcWritableBridge.readInteger(10, __dbResults);
    this.PROD_UNIT_OF_MEASURE = JdbcWritableBridge.readString(11, __dbResults);
    this.PROD_PACK_SIZE = JdbcWritableBridge.readString(12, __dbResults);
    this.SUPPLIER_ID = JdbcWritableBridge.readInteger(13, __dbResults);
    this.PROD_STATUS = JdbcWritableBridge.readString(14, __dbResults);
    this.PROD_LIST_PRICE = JdbcWritableBridge.readString(15, __dbResults);
    this.PROD_MIN_PRICE = JdbcWritableBridge.readString(16, __dbResults);
    this.PROD_TOTAL = JdbcWritableBridge.readString(17, __dbResults);
    this.PROD_TOTAL_ID = JdbcWritableBridge.readString(18, __dbResults);
    this.PROD_SRC_ID = JdbcWritableBridge.readString(19, __dbResults);
    this.PROD_EFF_FROM = JdbcWritableBridge.readString(20, __dbResults);
    this.PROD_EFF_TO = JdbcWritableBridge.readString(21, __dbResults);
    this.PROD_VALID = JdbcWritableBridge.readString(22, __dbResults);
  }
  public void loadLargeObjects(LargeObjectLoader __loader)
      throws SQLException, IOException, InterruptedException {
  }
  public void loadLargeObjects0(LargeObjectLoader __loader)
      throws SQLException, IOException, InterruptedException {
  }
  public void write(PreparedStatement __dbStmt) throws SQLException {
    write(__dbStmt, 0);
  }

  public int write(PreparedStatement __dbStmt, int __off) throws SQLException {
    JdbcWritableBridge.writeInteger(PROD_ID, 1 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(PROD_NAME, 2 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(PROD_DESC, 3 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(PROD_SUBCATEGORY, 4 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(PROD_SUBCATEGORY_ID, 5 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(PROD_SUBCATEGORY_DESC, 6 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(PROD_CATEGORY, 7 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(PROD_CATEGORY_ID, 8 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(PROD_CATEGORY_DESC, 9 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeInteger(PROD_WEIGHT_CLASS, 10 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(PROD_UNIT_OF_MEASURE, 11 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(PROD_PACK_SIZE, 12 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeInteger(SUPPLIER_ID, 13 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(PROD_STATUS, 14 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(PROD_LIST_PRICE, 15 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(PROD_MIN_PRICE, 16 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(PROD_TOTAL, 17 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(PROD_TOTAL_ID, 18 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(PROD_SRC_ID, 19 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(PROD_EFF_FROM, 20 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeString(PROD_EFF_TO, 21 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeString(PROD_VALID, 22 + __off, 12, __dbStmt);
    return 22;
  }
  public void write0(PreparedStatement __dbStmt, int __off) throws SQLException {
    JdbcWritableBridge.writeInteger(PROD_ID, 1 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(PROD_NAME, 2 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(PROD_DESC, 3 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(PROD_SUBCATEGORY, 4 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(PROD_SUBCATEGORY_ID, 5 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(PROD_SUBCATEGORY_DESC, 6 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(PROD_CATEGORY, 7 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(PROD_CATEGORY_ID, 8 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(PROD_CATEGORY_DESC, 9 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeInteger(PROD_WEIGHT_CLASS, 10 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(PROD_UNIT_OF_MEASURE, 11 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(PROD_PACK_SIZE, 12 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeInteger(SUPPLIER_ID, 13 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(PROD_STATUS, 14 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(PROD_LIST_PRICE, 15 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(PROD_MIN_PRICE, 16 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(PROD_TOTAL, 17 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(PROD_TOTAL_ID, 18 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(PROD_SRC_ID, 19 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(PROD_EFF_FROM, 20 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeString(PROD_EFF_TO, 21 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeString(PROD_VALID, 22 + __off, 12, __dbStmt);
  }
  public void readFields(DataInput __dataIn) throws IOException {
this.readFields0(__dataIn);  }
  public void readFields0(DataInput __dataIn) throws IOException {
    if (__dataIn.readBoolean()) { 
        this.PROD_ID = null;
    } else {
    this.PROD_ID = Integer.valueOf(__dataIn.readInt());
    }
    if (__dataIn.readBoolean()) { 
        this.PROD_NAME = null;
    } else {
    this.PROD_NAME = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.PROD_DESC = null;
    } else {
    this.PROD_DESC = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.PROD_SUBCATEGORY = null;
    } else {
    this.PROD_SUBCATEGORY = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.PROD_SUBCATEGORY_ID = null;
    } else {
    this.PROD_SUBCATEGORY_ID = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.PROD_SUBCATEGORY_DESC = null;
    } else {
    this.PROD_SUBCATEGORY_DESC = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.PROD_CATEGORY = null;
    } else {
    this.PROD_CATEGORY = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.PROD_CATEGORY_ID = null;
    } else {
    this.PROD_CATEGORY_ID = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.PROD_CATEGORY_DESC = null;
    } else {
    this.PROD_CATEGORY_DESC = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.PROD_WEIGHT_CLASS = null;
    } else {
    this.PROD_WEIGHT_CLASS = Integer.valueOf(__dataIn.readInt());
    }
    if (__dataIn.readBoolean()) { 
        this.PROD_UNIT_OF_MEASURE = null;
    } else {
    this.PROD_UNIT_OF_MEASURE = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.PROD_PACK_SIZE = null;
    } else {
    this.PROD_PACK_SIZE = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.SUPPLIER_ID = null;
    } else {
    this.SUPPLIER_ID = Integer.valueOf(__dataIn.readInt());
    }
    if (__dataIn.readBoolean()) { 
        this.PROD_STATUS = null;
    } else {
    this.PROD_STATUS = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.PROD_LIST_PRICE = null;
    } else {
    this.PROD_LIST_PRICE = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.PROD_MIN_PRICE = null;
    } else {
    this.PROD_MIN_PRICE = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.PROD_TOTAL = null;
    } else {
    this.PROD_TOTAL = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.PROD_TOTAL_ID = null;
    } else {
    this.PROD_TOTAL_ID = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.PROD_SRC_ID = null;
    } else {
    this.PROD_SRC_ID = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.PROD_EFF_FROM = null;
    } else {
    this.PROD_EFF_FROM = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.PROD_EFF_TO = null;
    } else {
    this.PROD_EFF_TO = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.PROD_VALID = null;
    } else {
    this.PROD_VALID = Text.readString(__dataIn);
    }
  }
  public void write(DataOutput __dataOut) throws IOException {
    if (null == this.PROD_ID) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeInt(this.PROD_ID);
    }
    if (null == this.PROD_NAME) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, PROD_NAME);
    }
    if (null == this.PROD_DESC) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, PROD_DESC);
    }
    if (null == this.PROD_SUBCATEGORY) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, PROD_SUBCATEGORY);
    }
    if (null == this.PROD_SUBCATEGORY_ID) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, PROD_SUBCATEGORY_ID);
    }
    if (null == this.PROD_SUBCATEGORY_DESC) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, PROD_SUBCATEGORY_DESC);
    }
    if (null == this.PROD_CATEGORY) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, PROD_CATEGORY);
    }
    if (null == this.PROD_CATEGORY_ID) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, PROD_CATEGORY_ID);
    }
    if (null == this.PROD_CATEGORY_DESC) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, PROD_CATEGORY_DESC);
    }
    if (null == this.PROD_WEIGHT_CLASS) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeInt(this.PROD_WEIGHT_CLASS);
    }
    if (null == this.PROD_UNIT_OF_MEASURE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, PROD_UNIT_OF_MEASURE);
    }
    if (null == this.PROD_PACK_SIZE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, PROD_PACK_SIZE);
    }
    if (null == this.SUPPLIER_ID) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeInt(this.SUPPLIER_ID);
    }
    if (null == this.PROD_STATUS) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, PROD_STATUS);
    }
    if (null == this.PROD_LIST_PRICE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, PROD_LIST_PRICE);
    }
    if (null == this.PROD_MIN_PRICE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, PROD_MIN_PRICE);
    }
    if (null == this.PROD_TOTAL) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, PROD_TOTAL);
    }
    if (null == this.PROD_TOTAL_ID) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, PROD_TOTAL_ID);
    }
    if (null == this.PROD_SRC_ID) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, PROD_SRC_ID);
    }
    if (null == this.PROD_EFF_FROM) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, PROD_EFF_FROM);
    }
    if (null == this.PROD_EFF_TO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, PROD_EFF_TO);
    }
    if (null == this.PROD_VALID) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, PROD_VALID);
    }
  }
  public void write0(DataOutput __dataOut) throws IOException {
    if (null == this.PROD_ID) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeInt(this.PROD_ID);
    }
    if (null == this.PROD_NAME) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, PROD_NAME);
    }
    if (null == this.PROD_DESC) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, PROD_DESC);
    }
    if (null == this.PROD_SUBCATEGORY) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, PROD_SUBCATEGORY);
    }
    if (null == this.PROD_SUBCATEGORY_ID) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, PROD_SUBCATEGORY_ID);
    }
    if (null == this.PROD_SUBCATEGORY_DESC) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, PROD_SUBCATEGORY_DESC);
    }
    if (null == this.PROD_CATEGORY) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, PROD_CATEGORY);
    }
    if (null == this.PROD_CATEGORY_ID) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, PROD_CATEGORY_ID);
    }
    if (null == this.PROD_CATEGORY_DESC) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, PROD_CATEGORY_DESC);
    }
    if (null == this.PROD_WEIGHT_CLASS) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeInt(this.PROD_WEIGHT_CLASS);
    }
    if (null == this.PROD_UNIT_OF_MEASURE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, PROD_UNIT_OF_MEASURE);
    }
    if (null == this.PROD_PACK_SIZE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, PROD_PACK_SIZE);
    }
    if (null == this.SUPPLIER_ID) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeInt(this.SUPPLIER_ID);
    }
    if (null == this.PROD_STATUS) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, PROD_STATUS);
    }
    if (null == this.PROD_LIST_PRICE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, PROD_LIST_PRICE);
    }
    if (null == this.PROD_MIN_PRICE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, PROD_MIN_PRICE);
    }
    if (null == this.PROD_TOTAL) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, PROD_TOTAL);
    }
    if (null == this.PROD_TOTAL_ID) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, PROD_TOTAL_ID);
    }
    if (null == this.PROD_SRC_ID) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, PROD_SRC_ID);
    }
    if (null == this.PROD_EFF_FROM) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, PROD_EFF_FROM);
    }
    if (null == this.PROD_EFF_TO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, PROD_EFF_TO);
    }
    if (null == this.PROD_VALID) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, PROD_VALID);
    }
  }
  private static final DelimiterSet __outputDelimiters = new DelimiterSet((char) 44, (char) 10, (char) 0, (char) 0, false);
  public String toString() {
    return toString(__outputDelimiters, true);
  }
  public String toString(DelimiterSet delimiters) {
    return toString(delimiters, true);
  }
  public String toString(boolean useRecordDelim) {
    return toString(__outputDelimiters, useRecordDelim);
  }
  public String toString(DelimiterSet delimiters, boolean useRecordDelim) {
    StringBuilder __sb = new StringBuilder();
    char fieldDelim = delimiters.getFieldsTerminatedBy();
    __sb.append(FieldFormatter.escapeAndEnclose(PROD_ID==null?"''":"" + PROD_ID, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(PROD_NAME==null?"''":PROD_NAME, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(PROD_DESC==null?"''":PROD_DESC, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(PROD_SUBCATEGORY==null?"''":PROD_SUBCATEGORY, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(PROD_SUBCATEGORY_ID==null?"''":PROD_SUBCATEGORY_ID, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(PROD_SUBCATEGORY_DESC==null?"''":PROD_SUBCATEGORY_DESC, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(PROD_CATEGORY==null?"''":PROD_CATEGORY, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(PROD_CATEGORY_ID==null?"''":PROD_CATEGORY_ID, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(PROD_CATEGORY_DESC==null?"''":PROD_CATEGORY_DESC, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(PROD_WEIGHT_CLASS==null?"''":"" + PROD_WEIGHT_CLASS, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(PROD_UNIT_OF_MEASURE==null?"''":PROD_UNIT_OF_MEASURE, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(PROD_PACK_SIZE==null?"''":PROD_PACK_SIZE, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(SUPPLIER_ID==null?"''":"" + SUPPLIER_ID, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(PROD_STATUS==null?"''":PROD_STATUS, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(PROD_LIST_PRICE==null?"''":PROD_LIST_PRICE, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(PROD_MIN_PRICE==null?"''":PROD_MIN_PRICE, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(PROD_TOTAL==null?"''":PROD_TOTAL, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(PROD_TOTAL_ID==null?"''":PROD_TOTAL_ID, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(PROD_SRC_ID==null?"''":PROD_SRC_ID, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(PROD_EFF_FROM==null?"''":PROD_EFF_FROM, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(PROD_EFF_TO==null?"''":PROD_EFF_TO, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(PROD_VALID==null?"''":PROD_VALID, delimiters));
    if (useRecordDelim) {
      __sb.append(delimiters.getLinesTerminatedBy());
    }
    return __sb.toString();
  }
  public void toString0(DelimiterSet delimiters, StringBuilder __sb, char fieldDelim) {
    __sb.append(FieldFormatter.escapeAndEnclose(PROD_ID==null?"''":"" + PROD_ID, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(PROD_NAME==null?"''":PROD_NAME, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(PROD_DESC==null?"''":PROD_DESC, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(PROD_SUBCATEGORY==null?"''":PROD_SUBCATEGORY, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(PROD_SUBCATEGORY_ID==null?"''":PROD_SUBCATEGORY_ID, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(PROD_SUBCATEGORY_DESC==null?"''":PROD_SUBCATEGORY_DESC, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(PROD_CATEGORY==null?"''":PROD_CATEGORY, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(PROD_CATEGORY_ID==null?"''":PROD_CATEGORY_ID, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(PROD_CATEGORY_DESC==null?"''":PROD_CATEGORY_DESC, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(PROD_WEIGHT_CLASS==null?"''":"" + PROD_WEIGHT_CLASS, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(PROD_UNIT_OF_MEASURE==null?"''":PROD_UNIT_OF_MEASURE, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(PROD_PACK_SIZE==null?"''":PROD_PACK_SIZE, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(SUPPLIER_ID==null?"''":"" + SUPPLIER_ID, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(PROD_STATUS==null?"''":PROD_STATUS, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(PROD_LIST_PRICE==null?"''":PROD_LIST_PRICE, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(PROD_MIN_PRICE==null?"''":PROD_MIN_PRICE, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(PROD_TOTAL==null?"''":PROD_TOTAL, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(PROD_TOTAL_ID==null?"''":PROD_TOTAL_ID, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(PROD_SRC_ID==null?"''":PROD_SRC_ID, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(PROD_EFF_FROM==null?"''":PROD_EFF_FROM, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(PROD_EFF_TO==null?"''":PROD_EFF_TO, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(PROD_VALID==null?"''":PROD_VALID, delimiters));
  }
  private static final DelimiterSet __inputDelimiters = new DelimiterSet((char) 44, (char) 10, (char) 0, (char) 0, false);
  private RecordParser __parser;
  public void parse(Text __record) throws RecordParser.ParseError {
    if (null == this.__parser) {
      this.__parser = new RecordParser(__inputDelimiters);
    }
    List<String> __fields = this.__parser.parseRecord(__record);
    __loadFromFields(__fields);
  }

  public void parse(CharSequence __record) throws RecordParser.ParseError {
    if (null == this.__parser) {
      this.__parser = new RecordParser(__inputDelimiters);
    }
    List<String> __fields = this.__parser.parseRecord(__record);
    __loadFromFields(__fields);
  }

  public void parse(byte [] __record) throws RecordParser.ParseError {
    if (null == this.__parser) {
      this.__parser = new RecordParser(__inputDelimiters);
    }
    List<String> __fields = this.__parser.parseRecord(__record);
    __loadFromFields(__fields);
  }

  public void parse(char [] __record) throws RecordParser.ParseError {
    if (null == this.__parser) {
      this.__parser = new RecordParser(__inputDelimiters);
    }
    List<String> __fields = this.__parser.parseRecord(__record);
    __loadFromFields(__fields);
  }

  public void parse(ByteBuffer __record) throws RecordParser.ParseError {
    if (null == this.__parser) {
      this.__parser = new RecordParser(__inputDelimiters);
    }
    List<String> __fields = this.__parser.parseRecord(__record);
    __loadFromFields(__fields);
  }

  public void parse(CharBuffer __record) throws RecordParser.ParseError {
    if (null == this.__parser) {
      this.__parser = new RecordParser(__inputDelimiters);
    }
    List<String> __fields = this.__parser.parseRecord(__record);
    __loadFromFields(__fields);
  }

  private void __loadFromFields(List<String> fields) {
    Iterator<String> __it = fields.listIterator();
    String __cur_str = null;
    try {
    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.PROD_ID = null; } else {
      this.PROD_ID = Integer.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.PROD_NAME = null; } else {
      this.PROD_NAME = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.PROD_DESC = null; } else {
      this.PROD_DESC = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.PROD_SUBCATEGORY = null; } else {
      this.PROD_SUBCATEGORY = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.PROD_SUBCATEGORY_ID = null; } else {
      this.PROD_SUBCATEGORY_ID = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.PROD_SUBCATEGORY_DESC = null; } else {
      this.PROD_SUBCATEGORY_DESC = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.PROD_CATEGORY = null; } else {
      this.PROD_CATEGORY = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.PROD_CATEGORY_ID = null; } else {
      this.PROD_CATEGORY_ID = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.PROD_CATEGORY_DESC = null; } else {
      this.PROD_CATEGORY_DESC = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.PROD_WEIGHT_CLASS = null; } else {
      this.PROD_WEIGHT_CLASS = Integer.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.PROD_UNIT_OF_MEASURE = null; } else {
      this.PROD_UNIT_OF_MEASURE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.PROD_PACK_SIZE = null; } else {
      this.PROD_PACK_SIZE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.SUPPLIER_ID = null; } else {
      this.SUPPLIER_ID = Integer.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.PROD_STATUS = null; } else {
      this.PROD_STATUS = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.PROD_LIST_PRICE = null; } else {
      this.PROD_LIST_PRICE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.PROD_MIN_PRICE = null; } else {
      this.PROD_MIN_PRICE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.PROD_TOTAL = null; } else {
      this.PROD_TOTAL = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.PROD_TOTAL_ID = null; } else {
      this.PROD_TOTAL_ID = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.PROD_SRC_ID = null; } else {
      this.PROD_SRC_ID = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.PROD_EFF_FROM = null; } else {
      this.PROD_EFF_FROM = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.PROD_EFF_TO = null; } else {
      this.PROD_EFF_TO = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.PROD_VALID = null; } else {
      this.PROD_VALID = __cur_str;
    }

    } catch (RuntimeException e) {    throw new RuntimeException("Can't parse input data: '" + __cur_str + "'", e);    }  }

  private void __loadFromFields0(Iterator<String> __it) {
    String __cur_str = null;
    try {
    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.PROD_ID = null; } else {
      this.PROD_ID = Integer.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.PROD_NAME = null; } else {
      this.PROD_NAME = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.PROD_DESC = null; } else {
      this.PROD_DESC = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.PROD_SUBCATEGORY = null; } else {
      this.PROD_SUBCATEGORY = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.PROD_SUBCATEGORY_ID = null; } else {
      this.PROD_SUBCATEGORY_ID = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.PROD_SUBCATEGORY_DESC = null; } else {
      this.PROD_SUBCATEGORY_DESC = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.PROD_CATEGORY = null; } else {
      this.PROD_CATEGORY = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.PROD_CATEGORY_ID = null; } else {
      this.PROD_CATEGORY_ID = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.PROD_CATEGORY_DESC = null; } else {
      this.PROD_CATEGORY_DESC = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.PROD_WEIGHT_CLASS = null; } else {
      this.PROD_WEIGHT_CLASS = Integer.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.PROD_UNIT_OF_MEASURE = null; } else {
      this.PROD_UNIT_OF_MEASURE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.PROD_PACK_SIZE = null; } else {
      this.PROD_PACK_SIZE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.SUPPLIER_ID = null; } else {
      this.SUPPLIER_ID = Integer.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.PROD_STATUS = null; } else {
      this.PROD_STATUS = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.PROD_LIST_PRICE = null; } else {
      this.PROD_LIST_PRICE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.PROD_MIN_PRICE = null; } else {
      this.PROD_MIN_PRICE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.PROD_TOTAL = null; } else {
      this.PROD_TOTAL = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.PROD_TOTAL_ID = null; } else {
      this.PROD_TOTAL_ID = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.PROD_SRC_ID = null; } else {
      this.PROD_SRC_ID = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.PROD_EFF_FROM = null; } else {
      this.PROD_EFF_FROM = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.PROD_EFF_TO = null; } else {
      this.PROD_EFF_TO = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.PROD_VALID = null; } else {
      this.PROD_VALID = __cur_str;
    }

    } catch (RuntimeException e) {    throw new RuntimeException("Can't parse input data: '" + __cur_str + "'", e);    }  }

  public Object clone() throws CloneNotSupportedException {
    story_nossh o = (story_nossh) super.clone();
    return o;
  }

  public void clone0(story_nossh o) throws CloneNotSupportedException {
  }

  public Map<String, Object> getFieldMap() {
    Map<String, Object> __sqoop$field_map = new HashMap<String, Object>();
    __sqoop$field_map.put("PROD_ID", this.PROD_ID);
    __sqoop$field_map.put("PROD_NAME", this.PROD_NAME);
    __sqoop$field_map.put("PROD_DESC", this.PROD_DESC);
    __sqoop$field_map.put("PROD_SUBCATEGORY", this.PROD_SUBCATEGORY);
    __sqoop$field_map.put("PROD_SUBCATEGORY_ID", this.PROD_SUBCATEGORY_ID);
    __sqoop$field_map.put("PROD_SUBCATEGORY_DESC", this.PROD_SUBCATEGORY_DESC);
    __sqoop$field_map.put("PROD_CATEGORY", this.PROD_CATEGORY);
    __sqoop$field_map.put("PROD_CATEGORY_ID", this.PROD_CATEGORY_ID);
    __sqoop$field_map.put("PROD_CATEGORY_DESC", this.PROD_CATEGORY_DESC);
    __sqoop$field_map.put("PROD_WEIGHT_CLASS", this.PROD_WEIGHT_CLASS);
    __sqoop$field_map.put("PROD_UNIT_OF_MEASURE", this.PROD_UNIT_OF_MEASURE);
    __sqoop$field_map.put("PROD_PACK_SIZE", this.PROD_PACK_SIZE);
    __sqoop$field_map.put("SUPPLIER_ID", this.SUPPLIER_ID);
    __sqoop$field_map.put("PROD_STATUS", this.PROD_STATUS);
    __sqoop$field_map.put("PROD_LIST_PRICE", this.PROD_LIST_PRICE);
    __sqoop$field_map.put("PROD_MIN_PRICE", this.PROD_MIN_PRICE);
    __sqoop$field_map.put("PROD_TOTAL", this.PROD_TOTAL);
    __sqoop$field_map.put("PROD_TOTAL_ID", this.PROD_TOTAL_ID);
    __sqoop$field_map.put("PROD_SRC_ID", this.PROD_SRC_ID);
    __sqoop$field_map.put("PROD_EFF_FROM", this.PROD_EFF_FROM);
    __sqoop$field_map.put("PROD_EFF_TO", this.PROD_EFF_TO);
    __sqoop$field_map.put("PROD_VALID", this.PROD_VALID);
    return __sqoop$field_map;
  }

  public void getFieldMap0(Map<String, Object> __sqoop$field_map) {
    __sqoop$field_map.put("PROD_ID", this.PROD_ID);
    __sqoop$field_map.put("PROD_NAME", this.PROD_NAME);
    __sqoop$field_map.put("PROD_DESC", this.PROD_DESC);
    __sqoop$field_map.put("PROD_SUBCATEGORY", this.PROD_SUBCATEGORY);
    __sqoop$field_map.put("PROD_SUBCATEGORY_ID", this.PROD_SUBCATEGORY_ID);
    __sqoop$field_map.put("PROD_SUBCATEGORY_DESC", this.PROD_SUBCATEGORY_DESC);
    __sqoop$field_map.put("PROD_CATEGORY", this.PROD_CATEGORY);
    __sqoop$field_map.put("PROD_CATEGORY_ID", this.PROD_CATEGORY_ID);
    __sqoop$field_map.put("PROD_CATEGORY_DESC", this.PROD_CATEGORY_DESC);
    __sqoop$field_map.put("PROD_WEIGHT_CLASS", this.PROD_WEIGHT_CLASS);
    __sqoop$field_map.put("PROD_UNIT_OF_MEASURE", this.PROD_UNIT_OF_MEASURE);
    __sqoop$field_map.put("PROD_PACK_SIZE", this.PROD_PACK_SIZE);
    __sqoop$field_map.put("SUPPLIER_ID", this.SUPPLIER_ID);
    __sqoop$field_map.put("PROD_STATUS", this.PROD_STATUS);
    __sqoop$field_map.put("PROD_LIST_PRICE", this.PROD_LIST_PRICE);
    __sqoop$field_map.put("PROD_MIN_PRICE", this.PROD_MIN_PRICE);
    __sqoop$field_map.put("PROD_TOTAL", this.PROD_TOTAL);
    __sqoop$field_map.put("PROD_TOTAL_ID", this.PROD_TOTAL_ID);
    __sqoop$field_map.put("PROD_SRC_ID", this.PROD_SRC_ID);
    __sqoop$field_map.put("PROD_EFF_FROM", this.PROD_EFF_FROM);
    __sqoop$field_map.put("PROD_EFF_TO", this.PROD_EFF_TO);
    __sqoop$field_map.put("PROD_VALID", this.PROD_VALID);
  }

  public void setField(String __fieldName, Object __fieldVal) {
    if (!setters.containsKey(__fieldName)) {
      throw new RuntimeException("No such field:"+__fieldName);
    }
    setters.get(__fieldName).setField(__fieldVal);
  }

}
