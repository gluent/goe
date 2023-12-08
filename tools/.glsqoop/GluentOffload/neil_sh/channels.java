// ORM class for table 'SH.CHANNELS'
// WARNING: This class is AUTO-GENERATED. Modify at your own risk.
//
// Debug information:
// Generated date: Tue May 18 10:16:49 UTC 2021
// For connector: org.apache.sqoop.manager.oracle.OraOopConnManager
package GluentOffload.neil_sh;
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

public class channels extends SqoopRecord  implements DBWritable, Writable {
  private final int PROTOCOL_VERSION = 3;
  public int getClassFormatVersion() { return PROTOCOL_VERSION; }
  public static interface FieldSetterCommand {    void setField(Object value);  }  protected ResultSet __cur_result_set;
  private Map<String, FieldSetterCommand> setters = new HashMap<String, FieldSetterCommand>();
  private void init0() {
    setters.put("CHANNEL_ID", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        CHANNEL_ID = (String)value;
      }
    });
    setters.put("CHANNEL_DESC", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        CHANNEL_DESC = (String)value;
      }
    });
    setters.put("CHANNEL_CLASS", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        CHANNEL_CLASS = (String)value;
      }
    });
    setters.put("CHANNEL_CLASS_ID", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        CHANNEL_CLASS_ID = (String)value;
      }
    });
    setters.put("CHANNEL_TOTAL", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        CHANNEL_TOTAL = (String)value;
      }
    });
    setters.put("CHANNEL_TOTAL_ID", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        CHANNEL_TOTAL_ID = (String)value;
      }
    });
  }
  public channels() {
    init0();
  }
  private String CHANNEL_ID;
  public String get_CHANNEL_ID() {
    return CHANNEL_ID;
  }
  public void set_CHANNEL_ID(String CHANNEL_ID) {
    this.CHANNEL_ID = CHANNEL_ID;
  }
  public channels with_CHANNEL_ID(String CHANNEL_ID) {
    this.CHANNEL_ID = CHANNEL_ID;
    return this;
  }
  private String CHANNEL_DESC;
  public String get_CHANNEL_DESC() {
    return CHANNEL_DESC;
  }
  public void set_CHANNEL_DESC(String CHANNEL_DESC) {
    this.CHANNEL_DESC = CHANNEL_DESC;
  }
  public channels with_CHANNEL_DESC(String CHANNEL_DESC) {
    this.CHANNEL_DESC = CHANNEL_DESC;
    return this;
  }
  private String CHANNEL_CLASS;
  public String get_CHANNEL_CLASS() {
    return CHANNEL_CLASS;
  }
  public void set_CHANNEL_CLASS(String CHANNEL_CLASS) {
    this.CHANNEL_CLASS = CHANNEL_CLASS;
  }
  public channels with_CHANNEL_CLASS(String CHANNEL_CLASS) {
    this.CHANNEL_CLASS = CHANNEL_CLASS;
    return this;
  }
  private String CHANNEL_CLASS_ID;
  public String get_CHANNEL_CLASS_ID() {
    return CHANNEL_CLASS_ID;
  }
  public void set_CHANNEL_CLASS_ID(String CHANNEL_CLASS_ID) {
    this.CHANNEL_CLASS_ID = CHANNEL_CLASS_ID;
  }
  public channels with_CHANNEL_CLASS_ID(String CHANNEL_CLASS_ID) {
    this.CHANNEL_CLASS_ID = CHANNEL_CLASS_ID;
    return this;
  }
  private String CHANNEL_TOTAL;
  public String get_CHANNEL_TOTAL() {
    return CHANNEL_TOTAL;
  }
  public void set_CHANNEL_TOTAL(String CHANNEL_TOTAL) {
    this.CHANNEL_TOTAL = CHANNEL_TOTAL;
  }
  public channels with_CHANNEL_TOTAL(String CHANNEL_TOTAL) {
    this.CHANNEL_TOTAL = CHANNEL_TOTAL;
    return this;
  }
  private String CHANNEL_TOTAL_ID;
  public String get_CHANNEL_TOTAL_ID() {
    return CHANNEL_TOTAL_ID;
  }
  public void set_CHANNEL_TOTAL_ID(String CHANNEL_TOTAL_ID) {
    this.CHANNEL_TOTAL_ID = CHANNEL_TOTAL_ID;
  }
  public channels with_CHANNEL_TOTAL_ID(String CHANNEL_TOTAL_ID) {
    this.CHANNEL_TOTAL_ID = CHANNEL_TOTAL_ID;
    return this;
  }
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof channels)) {
      return false;
    }
    channels that = (channels) o;
    boolean equal = true;
    equal = equal && (this.CHANNEL_ID == null ? that.CHANNEL_ID == null : this.CHANNEL_ID.equals(that.CHANNEL_ID));
    equal = equal && (this.CHANNEL_DESC == null ? that.CHANNEL_DESC == null : this.CHANNEL_DESC.equals(that.CHANNEL_DESC));
    equal = equal && (this.CHANNEL_CLASS == null ? that.CHANNEL_CLASS == null : this.CHANNEL_CLASS.equals(that.CHANNEL_CLASS));
    equal = equal && (this.CHANNEL_CLASS_ID == null ? that.CHANNEL_CLASS_ID == null : this.CHANNEL_CLASS_ID.equals(that.CHANNEL_CLASS_ID));
    equal = equal && (this.CHANNEL_TOTAL == null ? that.CHANNEL_TOTAL == null : this.CHANNEL_TOTAL.equals(that.CHANNEL_TOTAL));
    equal = equal && (this.CHANNEL_TOTAL_ID == null ? that.CHANNEL_TOTAL_ID == null : this.CHANNEL_TOTAL_ID.equals(that.CHANNEL_TOTAL_ID));
    return equal;
  }
  public boolean equals0(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof channels)) {
      return false;
    }
    channels that = (channels) o;
    boolean equal = true;
    equal = equal && (this.CHANNEL_ID == null ? that.CHANNEL_ID == null : this.CHANNEL_ID.equals(that.CHANNEL_ID));
    equal = equal && (this.CHANNEL_DESC == null ? that.CHANNEL_DESC == null : this.CHANNEL_DESC.equals(that.CHANNEL_DESC));
    equal = equal && (this.CHANNEL_CLASS == null ? that.CHANNEL_CLASS == null : this.CHANNEL_CLASS.equals(that.CHANNEL_CLASS));
    equal = equal && (this.CHANNEL_CLASS_ID == null ? that.CHANNEL_CLASS_ID == null : this.CHANNEL_CLASS_ID.equals(that.CHANNEL_CLASS_ID));
    equal = equal && (this.CHANNEL_TOTAL == null ? that.CHANNEL_TOTAL == null : this.CHANNEL_TOTAL.equals(that.CHANNEL_TOTAL));
    equal = equal && (this.CHANNEL_TOTAL_ID == null ? that.CHANNEL_TOTAL_ID == null : this.CHANNEL_TOTAL_ID.equals(that.CHANNEL_TOTAL_ID));
    return equal;
  }
  public void readFields(ResultSet __dbResults) throws SQLException {
    this.__cur_result_set = __dbResults;
    this.CHANNEL_ID = JdbcWritableBridge.readString(1, __dbResults);
    this.CHANNEL_DESC = JdbcWritableBridge.readString(2, __dbResults);
    this.CHANNEL_CLASS = JdbcWritableBridge.readString(3, __dbResults);
    this.CHANNEL_CLASS_ID = JdbcWritableBridge.readString(4, __dbResults);
    this.CHANNEL_TOTAL = JdbcWritableBridge.readString(5, __dbResults);
    this.CHANNEL_TOTAL_ID = JdbcWritableBridge.readString(6, __dbResults);
  }
  public void readFields0(ResultSet __dbResults) throws SQLException {
    this.CHANNEL_ID = JdbcWritableBridge.readString(1, __dbResults);
    this.CHANNEL_DESC = JdbcWritableBridge.readString(2, __dbResults);
    this.CHANNEL_CLASS = JdbcWritableBridge.readString(3, __dbResults);
    this.CHANNEL_CLASS_ID = JdbcWritableBridge.readString(4, __dbResults);
    this.CHANNEL_TOTAL = JdbcWritableBridge.readString(5, __dbResults);
    this.CHANNEL_TOTAL_ID = JdbcWritableBridge.readString(6, __dbResults);
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
    JdbcWritableBridge.writeString(CHANNEL_ID, 1 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(CHANNEL_DESC, 2 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(CHANNEL_CLASS, 3 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(CHANNEL_CLASS_ID, 4 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(CHANNEL_TOTAL, 5 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(CHANNEL_TOTAL_ID, 6 + __off, 2, __dbStmt);
    return 6;
  }
  public void write0(PreparedStatement __dbStmt, int __off) throws SQLException {
    JdbcWritableBridge.writeString(CHANNEL_ID, 1 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(CHANNEL_DESC, 2 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(CHANNEL_CLASS, 3 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(CHANNEL_CLASS_ID, 4 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(CHANNEL_TOTAL, 5 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(CHANNEL_TOTAL_ID, 6 + __off, 2, __dbStmt);
  }
  public void readFields(DataInput __dataIn) throws IOException {
this.readFields0(__dataIn);  }
  public void readFields0(DataInput __dataIn) throws IOException {
    if (__dataIn.readBoolean()) { 
        this.CHANNEL_ID = null;
    } else {
    this.CHANNEL_ID = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.CHANNEL_DESC = null;
    } else {
    this.CHANNEL_DESC = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.CHANNEL_CLASS = null;
    } else {
    this.CHANNEL_CLASS = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.CHANNEL_CLASS_ID = null;
    } else {
    this.CHANNEL_CLASS_ID = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.CHANNEL_TOTAL = null;
    } else {
    this.CHANNEL_TOTAL = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.CHANNEL_TOTAL_ID = null;
    } else {
    this.CHANNEL_TOTAL_ID = Text.readString(__dataIn);
    }
  }
  public void write(DataOutput __dataOut) throws IOException {
    if (null == this.CHANNEL_ID) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, CHANNEL_ID);
    }
    if (null == this.CHANNEL_DESC) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, CHANNEL_DESC);
    }
    if (null == this.CHANNEL_CLASS) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, CHANNEL_CLASS);
    }
    if (null == this.CHANNEL_CLASS_ID) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, CHANNEL_CLASS_ID);
    }
    if (null == this.CHANNEL_TOTAL) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, CHANNEL_TOTAL);
    }
    if (null == this.CHANNEL_TOTAL_ID) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, CHANNEL_TOTAL_ID);
    }
  }
  public void write0(DataOutput __dataOut) throws IOException {
    if (null == this.CHANNEL_ID) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, CHANNEL_ID);
    }
    if (null == this.CHANNEL_DESC) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, CHANNEL_DESC);
    }
    if (null == this.CHANNEL_CLASS) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, CHANNEL_CLASS);
    }
    if (null == this.CHANNEL_CLASS_ID) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, CHANNEL_CLASS_ID);
    }
    if (null == this.CHANNEL_TOTAL) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, CHANNEL_TOTAL);
    }
    if (null == this.CHANNEL_TOTAL_ID) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, CHANNEL_TOTAL_ID);
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
    __sb.append(FieldFormatter.escapeAndEnclose(CHANNEL_ID==null?"''":CHANNEL_ID, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(CHANNEL_DESC==null?"''":CHANNEL_DESC, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(CHANNEL_CLASS==null?"''":CHANNEL_CLASS, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(CHANNEL_CLASS_ID==null?"''":CHANNEL_CLASS_ID, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(CHANNEL_TOTAL==null?"''":CHANNEL_TOTAL, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(CHANNEL_TOTAL_ID==null?"''":CHANNEL_TOTAL_ID, delimiters));
    if (useRecordDelim) {
      __sb.append(delimiters.getLinesTerminatedBy());
    }
    return __sb.toString();
  }
  public void toString0(DelimiterSet delimiters, StringBuilder __sb, char fieldDelim) {
    __sb.append(FieldFormatter.escapeAndEnclose(CHANNEL_ID==null?"''":CHANNEL_ID, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(CHANNEL_DESC==null?"''":CHANNEL_DESC, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(CHANNEL_CLASS==null?"''":CHANNEL_CLASS, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(CHANNEL_CLASS_ID==null?"''":CHANNEL_CLASS_ID, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(CHANNEL_TOTAL==null?"''":CHANNEL_TOTAL, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(CHANNEL_TOTAL_ID==null?"''":CHANNEL_TOTAL_ID, delimiters));
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
    if (__cur_str.equals("null")) { this.CHANNEL_ID = null; } else {
      this.CHANNEL_ID = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.CHANNEL_DESC = null; } else {
      this.CHANNEL_DESC = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.CHANNEL_CLASS = null; } else {
      this.CHANNEL_CLASS = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.CHANNEL_CLASS_ID = null; } else {
      this.CHANNEL_CLASS_ID = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.CHANNEL_TOTAL = null; } else {
      this.CHANNEL_TOTAL = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.CHANNEL_TOTAL_ID = null; } else {
      this.CHANNEL_TOTAL_ID = __cur_str;
    }

    } catch (RuntimeException e) {    throw new RuntimeException("Can't parse input data: '" + __cur_str + "'", e);    }  }

  private void __loadFromFields0(Iterator<String> __it) {
    String __cur_str = null;
    try {
    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.CHANNEL_ID = null; } else {
      this.CHANNEL_ID = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.CHANNEL_DESC = null; } else {
      this.CHANNEL_DESC = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.CHANNEL_CLASS = null; } else {
      this.CHANNEL_CLASS = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.CHANNEL_CLASS_ID = null; } else {
      this.CHANNEL_CLASS_ID = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.CHANNEL_TOTAL = null; } else {
      this.CHANNEL_TOTAL = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.CHANNEL_TOTAL_ID = null; } else {
      this.CHANNEL_TOTAL_ID = __cur_str;
    }

    } catch (RuntimeException e) {    throw new RuntimeException("Can't parse input data: '" + __cur_str + "'", e);    }  }

  public Object clone() throws CloneNotSupportedException {
    channels o = (channels) super.clone();
    return o;
  }

  public void clone0(channels o) throws CloneNotSupportedException {
  }

  public Map<String, Object> getFieldMap() {
    Map<String, Object> __sqoop$field_map = new HashMap<String, Object>();
    __sqoop$field_map.put("CHANNEL_ID", this.CHANNEL_ID);
    __sqoop$field_map.put("CHANNEL_DESC", this.CHANNEL_DESC);
    __sqoop$field_map.put("CHANNEL_CLASS", this.CHANNEL_CLASS);
    __sqoop$field_map.put("CHANNEL_CLASS_ID", this.CHANNEL_CLASS_ID);
    __sqoop$field_map.put("CHANNEL_TOTAL", this.CHANNEL_TOTAL);
    __sqoop$field_map.put("CHANNEL_TOTAL_ID", this.CHANNEL_TOTAL_ID);
    return __sqoop$field_map;
  }

  public void getFieldMap0(Map<String, Object> __sqoop$field_map) {
    __sqoop$field_map.put("CHANNEL_ID", this.CHANNEL_ID);
    __sqoop$field_map.put("CHANNEL_DESC", this.CHANNEL_DESC);
    __sqoop$field_map.put("CHANNEL_CLASS", this.CHANNEL_CLASS);
    __sqoop$field_map.put("CHANNEL_CLASS_ID", this.CHANNEL_CLASS_ID);
    __sqoop$field_map.put("CHANNEL_TOTAL", this.CHANNEL_TOTAL);
    __sqoop$field_map.put("CHANNEL_TOTAL_ID", this.CHANNEL_TOTAL_ID);
  }

  public void setField(String __fieldName, Object __fieldVal) {
    if (!setters.containsKey(__fieldName)) {
      throw new RuntimeException("No such field:"+__fieldName);
    }
    setters.get(__fieldName).setField(__fieldVal);
  }

}
