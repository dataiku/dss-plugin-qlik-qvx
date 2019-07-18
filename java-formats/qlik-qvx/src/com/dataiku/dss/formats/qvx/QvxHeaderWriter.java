package com.dataiku.dss.formats.qvx;

import java.io.IOException;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;

import com.dataiku.dip.coremodel.SchemaColumn;

public class QvxHeaderWriter {
    private OutputStream os;
    private List<QvxField> fields;

    public QvxHeaderWriter(OutputStream os) {
        this.os = os;
    }

    public void setSchema(List<SchemaColumn> schemaColumns) {
        this.fields = new ArrayList<>(schemaColumns.size());
        for (SchemaColumn col: schemaColumns) {
            fields.add(makeQVXField(col));
        }
    }

    public void write() throws UnsupportedEncodingException, IOException {
        assert(this.fields != null);
        write("<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n" +
                "<QvxTableHeader>\n" +
                "\t<MajorVersion>1</MajorVersion>\n" +
                "\t<MinorVersion>0</MinorVersion>\n" +
                "\t<TableName>Dataset</TableName>\n" +
                "\t<UsesSeparatorByte>0</UsesSeparatorByte>\n" +
                "\t<BlockSize>0</BlockSize>\n" +
                "\t<Fields>\n");
        for (QvxField field: fields) {
            writeField(field);
        }
        write("\t</Fields>\n" + "</QvxTableHeader>");
        // Header must be followed by 0x00
        os.write((byte)0);
    }

    private enum QvxFieldType {
        QVX_TEXT,
        QVX_SIGNED_INTEGER,
        QVX_IEEE_REAL
    }

    private class QvxField {
        String name;
        QvxFieldType type;
        int byteWidth;
        boolean isDate;

        QvxField(String name, QvxFieldType type, int byteWidth) {
            this.name = name;
            this.type = type;
            this.byteWidth = byteWidth;
        }

        QvxField(String name, QvxFieldType type, int byteWidth, boolean isDate) {
            this(name, type, byteWidth);
            this.isDate = true;
        }
    }

    private QvxField makeQVXField(SchemaColumn col) {
        switch(col.getType()) {
        case BOOLEAN: // qlik does not have boolean type
            return new QvxField(col.getName(), QvxFieldType.QVX_SIGNED_INTEGER, 1);
        case TINYINT:
            return new QvxField(col.getName(), QvxFieldType.QVX_SIGNED_INTEGER, 1);
        case SMALLINT:
            return new QvxField(col.getName(), QvxFieldType.QVX_SIGNED_INTEGER, 2);
        case INT:
            return new QvxField(col.getName(), QvxFieldType.QVX_SIGNED_INTEGER, 4);
        case BIGINT:
            return new QvxField(col.getName(), QvxFieldType.QVX_SIGNED_INTEGER, 8);
        case FLOAT:
            return new QvxField(col.getName(), QvxFieldType.QVX_IEEE_REAL, 4);
        case DOUBLE:
            return new QvxField(col.getName(), QvxFieldType.QVX_IEEE_REAL, 8);
        case STRING:
            return new QvxField(col.getName(), QvxFieldType.QVX_TEXT, -1);
        case DATE:
            return new QvxField(col.getName(), QvxFieldType.QVX_TEXT, -1, true);
        case ARRAY:
        case MAP:
        case OBJECT:
        case GEOMETRY:
        case GEOPOINT:
            // Nothing specific implemented, write as string
            break;
        }
        return new QvxField(col.getName(), QvxFieldType.QVX_TEXT, -1);
    }

    private void writeField(QvxField field) throws UnsupportedEncodingException, IOException {
        write("\t\t<QvxFieldHeader>\n" +
                "\t\t\t<FieldName>"+field.name+"</FieldName>\n" + //TODO escape
                "\t\t\t<Type>"+field.type+"</Type>\n" +
                "\t\t\t<NullRepresentation>QVX_NULL_FLAG_SUPPRESS_DATA</NullRepresentation>\n");

        if (field.isDate) {
            write(  "\t\t\t<Extent>QVX_ZERO_TERMINATED</Extent>\n" +
                    "\t\t\t<FieldAttrType><Type>DATE</Type><Fmt>yyyy-mm-ddThh:mm:ss.fffZ</Fmt></FieldAttrType>\n"
                    );
        } else if (field.type == QvxFieldType.QVX_TEXT) {
            write(  "\t\t\t<Extent>QVX_ZERO_TERMINATED</Extent>\n" +
                    "\t\t\t<FieldAttrType><Type>ASCII</Type></FieldAttrType>\n"
                    );
        } else {
            write(  "\t\t\t<Extent>QVX_FIX</Extent>\n"+
                    "\t\t\t<BigEndian>0</BigEndian>\n" +
                    "\t\t\t<ByteWidth>"+field.byteWidth+"</ByteWidth>\n"
                    );
        }
        write("\t\t</QvxFieldHeader>\n");
    }

    private void write(String str) throws UnsupportedEncodingException, IOException {
        os.write(str.getBytes("UTF-8"));
    }
}
