package com.dataiku.dss.formats.qvx;

import com.dataiku.dip.plugin.CustomFormat;
import com.dataiku.dip.plugin.CustomFormatInput;
import com.dataiku.dip.plugin.CustomFormatOutput;
import com.dataiku.dip.plugin.CustomFormatSchemaDetector;
import com.dataiku.dip.utils.NotImplementedException;
import com.google.gson.JsonObject;

public class QvxFormat implements CustomFormat {
    @Override
    public CustomFormatInput getReader(JsonObject config, JsonObject pluginConfig) {
        throw new NotImplementedException();
    }

    @Override
    public CustomFormatOutput getWriter(JsonObject config, JsonObject pluginConfig) {
        return new QvxFormatOutput();
    }

    @Override
    public CustomFormatSchemaDetector getDetector(JsonObject config, JsonObject pluginConfig) {
        throw new NotImplementedException();
    }
}
