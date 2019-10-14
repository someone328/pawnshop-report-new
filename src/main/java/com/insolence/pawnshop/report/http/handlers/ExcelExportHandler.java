package com.insolence.pawnshop.report.http.handlers;

import io.vertx.core.Handler;
import io.vertx.reactivex.core.buffer.Buffer;
import io.vertx.reactivex.ext.web.RoutingContext;
import lombok.extern.slf4j.Slf4j;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

@Slf4j
public class ExcelExportHandler implements Handler<RoutingContext> {
    @Override
    public void handle(RoutingContext rc) {
        try {
            HSSFWorkbook xls = new HSSFWorkbook();
            xls.createSheet();
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            xls.write(baos);
            rc.response()
                    .putHeader("Content-Type", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet; charset=UTF-8")
                    .putHeader("Content-Disposition", "attachment; filename=\"report.xlsx\"")
                    .end(Buffer.buffer(baos.toByteArray()));
        } catch (IOException e) {
            log.error("Excel export error.", e);
            rc.fail(500, new Exception("Ошибка экспорта в excel"));
        }
    }
}
