package com.dataiku.dss.formats.qvx;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import com.dataiku.dip.coremodel.Schema;
import com.dataiku.dip.coremodel.SchemaColumn;
import com.dataiku.dip.datalayer.Column;
import com.dataiku.dip.datalayer.ColumnFactory;
import com.dataiku.dip.datalayer.Row;
import com.dataiku.dip.datasets.Type;
import com.dataiku.dip.io.BinaryStreamEncoder;
import com.dataiku.dip.plugin.CustomFormatOutput;
import com.dataiku.dip.warnings.WarningsContext;
import com.dataiku.dip.warnings.WarningsContext.WarningType;

public class QvxFormatOutput implements CustomFormatOutput {

    private List<SchemaColumn> schemaColumns;
    private List<Column> columns = new ArrayList<>();
    private WarningsContext wc;
    private OutputStream os;
    private BinaryStreamEncoder bse;

    @Override
    public void header(ColumnFactory cf, OutputStream os) throws Exception {
        this.os = os;
        this.bse = new BinaryStreamEncoder(os);
        for (SchemaColumn sc : this.schemaColumns) {
            this.columns.add(cf.column(sc.getName()));
        }
        QvxHeaderWriter writer = new QvxHeaderWriter(os);
        writer.setSchema(schemaColumns);
        writer.write();
    }

    @Override
    public void format(Row row, ColumnFactory cf, OutputStream os) throws Exception {
        assert(this.os == os);
        try {
            for (int i = 0; i < columns.size(); i++) {
                Column columnHandle = columns.get(i);
                String rawCellValue = row.get(columnHandle);
                writeNullable(rawCellValue, schemaColumns.get(i));
            }
        } catch (Exception e) {
            wc.addWarning(WarningType.OUTPUT_DATA_BAD_TYPE, "Bad data "+e.getMessage(), logger);
        }
    }

    private void writeNullable(String rawCellValue, SchemaColumn schemaColumn) throws IOException {
        boolean isNull = (rawCellValue == null);

        // Turn empty strings into NULLs if the type is not string (float, double, etc...)
        if (!isNull && rawCellValue instanceof String && schemaColumn.getType() != Type.STRING) {
            isNull = StringUtils.isBlank(rawCellValue);
            if ("null".equals(rawCellValue) && (schemaColumn.getType() == Type.ARRAY || schemaColumn.getType() == Type.MAP)) {
                isNull = true;
            }
        }
        if (isNull) {
            os.write((byte)1);
        } else {
            assert(rawCellValue != null);
            switch(schemaColumn.getType()) {
            case BOOLEAN: // qlik does not have boolean type
                os.write((byte)0);
                boolean booleanVal = "true".equals(rawCellValue) || "True".equals(rawCellValue) || "TRUE".equals(rawCellValue); // All other to false...
                byte byteVal = (byte)(booleanVal ? 1 : 0);
                os.write(byteVal);
                break;
            case TINYINT:
                os.write((byte)0);
                os.write(Byte.parseByte(rawCellValue));
                break;
            case SMALLINT:
                os.write((byte)0);
                bse.writeLE16(Short.parseShort(rawCellValue));
                break;
            case INT:
                os.write((byte)0);
                bse.writeLE32(Integer.parseInt(rawCellValue));
                break;
            case BIGINT:
                os.write((byte)0);
                bse.writeLE64(Long.parseLong(rawCellValue));
                break;
            case FLOAT:
                os.write((byte)0);
                bse.writeFloat(Float.parseFloat(rawCellValue));
                break;
            case DOUBLE:
                os.write((byte)0);
                bse.writeDouble(Double.parseDouble(rawCellValue));
                break;
            case STRING:
            case DATE: // dates are parsed by qlik since we provided a format in the header, we just write them as strings
            case ARRAY: // Other types have nothing specific implemented, written as strings
            case MAP:
            case OBJECT:
            case GEOMETRY:
            case GEOPOINT:
                os.write((byte)0); // leading 0 byte, as for other types means not empty (QVX_NULL_FLAG_SUPPRESS_DATA mode)
                os.write(rawCellValue.getBytes("UTF-8"));
                os.write((byte)0);
                break;
            }
        }
    }

    @Override
    public void footer(ColumnFactory cf, OutputStream os) throws Exception {
        // no footer in QVX
    }

    @Override
    public void cancel(OutputStream os) throws Exception {
        // nothing to cleanup
    }

    @Override
    public void setOutputSchema(Schema schema) {
        this.schemaColumns = schema.getColumns();
    }

    @Override
    public void setWarningsContext(WarningsContext warningsContext) {
        this.wc = warningsContext;
    }

    @Override
    public void close() throws Exception {
        this.bse.close();
    }

    private static final Logger logger = Logger.getLogger("dssplugin.qvx.output");
}
