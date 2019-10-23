package com.insolence.pawnshop.report.http.handlers;

import io.reactivex.Observable;
import io.vertx.core.Handler;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.reactivex.core.buffer.Buffer;
import io.vertx.reactivex.core.eventbus.Message;
import io.vertx.reactivex.ext.web.RoutingContext;
import lombok.extern.slf4j.Slf4j;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;

import java.io.ByteArrayOutputStream;

@Slf4j
public class ExcelExportHandler implements Handler<RoutingContext> {
    @Override
    public void handle(RoutingContext rc) {
        HSSFWorkbook xls = new HSSFWorkbook();
        JsonObject requestBody = rc.getBodyAsJson();
        String branchId = requestBody.getString("branchId");
        Long dateFrom = requestBody.getLong("dateFrom");
        Long dateTo = requestBody.getLong("dateTo");
        System.out.println("branchID:" + branchId);
        System.out.println("dateFrom:" + dateFrom);
        System.out.println("dateTo:" + dateTo);

        rc.vertx().eventBus()
                .rxRequest(
                        "crud.get",
                        new JsonObject().put("_id", branchId),
                        new DeliveryOptions().addHeader("objectType", CrudHandler.SupportedObjectTypes.BRANCH.name().toLowerCase())

                )
                .map(Message::body)
                .flatMapObservable(branchList -> Observable.fromIterable((JsonArray) branchList))
                .doOnEach(e -> System.out.println(e))
                .map(json -> xls.createSheet(((JsonObject) json).getString("name", "")))
                .subscribe(
                        success -> {

                        },
                        error -> {
                            log.error("Excel export error.", error);
                            //rc.fail(500, new Exception("Ошибка экспорта в excel"));
                        },
                        () -> {
                            ByteArrayOutputStream baos = new ByteArrayOutputStream();
                            xls.write(baos);
                            rc.response()
                                    .putHeader("Content-Type", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet; charset=UTF-8")
                                    .putHeader("Content-Disposition", "attachment; filename=\"report.xlsx\"")
                                    .end(Buffer.buffer(baos.toByteArray()));
                        }
                );
    }
}
