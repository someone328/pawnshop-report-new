package com.insolence.pawnshop.report.http.handlers;

import io.vertx.core.Handler;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.mongo.AggregateOptions;
import io.vertx.reactivex.core.buffer.Buffer;
import io.vertx.reactivex.ext.mongo.MongoClient;
import io.vertx.reactivex.ext.web.RoutingContext;
import lombok.extern.slf4j.Slf4j;
import org.apache.poi.hssf.usermodel.*;
import org.apache.poi.hssf.util.HSSFColor;
import org.apache.poi.poifs.filesystem.POIFSFileSystem;
import org.apache.poi.ss.usermodel.*;
import org.apache.poi.ss.util.CellRangeAddress;

import java.awt.Color;
import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
public class ExcelExportHandler implements Handler<RoutingContext> {
    private MongoClient client;
    private List<HSSFCellStyle> cellStyles = new ArrayList<>();
    String query = "[\n" +
            "{\"$match\":{    \n" +
            "    \"branch\":\"%s\",\n" +
            "     \"date\":{\"$gte\":%s, \"$lte\":%s}}\n" +
            "     },\n" +
            "{\"$lookup\":{\n" +
            "    \"from\":\"user\",\n" +
            "    \"localField\":\"user\",\n" +
            "    \"foreignField\":\"_id\",\n" +
            "    \"as\":\"username\"\n" +
            "    }},\n" +
            "    {\"$unwind\":{\n" +
            "         \"path\":\"$username\"\n" +
            "         }},\n" +
            "{\"$group\":{\n" +
            "    \"_id\":\"$_id\",\n" +
            "    \"report\":{\"$push\":{\n" +
            "        \"utc\":\"$date\",\n" +
            "        \"date\":{\"$dateToString\":{\n" +
            "            \"date\":{\"$toDate\":\"$date\"},\n" +
            "            \"format\":\"%s\"}},\n" +
            "        \"username\":\"$username.name\",\n" +
            "        \"loanersAsset\":\"TODO\",\n" +
            "        \"loanersPawned\":\"$loanersPawned\",\n" +
            "        \"loanersBought\":\"$loanersBought\",\n" +
            "        \"pawnersRate\":\"TODO\",\n" +
            "        \"pawnersRatePercent\":\"TODO\",\n" +
            "        \"cashboxMorning\":\"TODO\",\n" +
            "        \"volume\":\"TODO\",\n" +
            "        \"loanedRub\":\"$loanedRub\",\n" +
            "        \"repayedRub\":\"$repayedRub\",\n" +
            "        \"totalPercentRecieved\":\"TODO\",\n" +
            "        \"percentRecieved\":\"TODO\",\n" +
            "        \"dailyGrowth\":\"TODO\",\n" +
            "        \"dailyGrowthPercent\":\"TODO\",\n" +
            "        \"goldBalance\":\"TODO\",\n" +
            "        \"goldBought\":\"$goldBought\",\n" +
            "        \"goldSold\":\"$goldSold\",\n" +
            "        \"silverBalance\":\"TODO\",\n" +
            "        \"silverBought\":\"$silverBought\",\n" +
            "        \"silverSold\":\"$silverSold\",\n" +
            "        \"diamondsBalance\":\"TODO\",\n" +
            "        \"diamondsBought\":\"$diamondBought\",\n" +
            "        \"diamondsSold\":\"$diamondSold\",\n" +
            "        \"goodsBalance\":\"TODO\",\n" +
            "        \"goodsBought\":\"$goodsBought\",\n" +
            "        \"goodsSold\":\"$goodsSold\",\n" +
            "        \"tradesActive\":\"$tradesActive\",\n" +
            "        \"silverTradeWeight\":\"$silverTradeWeight\",\n" +
            "        \"goldTradeWeight\":\"$goldTradeWeight\",\n" +
            "        \"metalTradeSum\":{\"$add\":[{\"$toDouble\":\"$goldTradeSum\"},{\"$toDouble\":\"$silverTradeSum\"}]},    \n" +
            "        \"goodsTradeSum\":\"$goodsTradeSum\",\n" +
            "        \"auctionAmount\":\"$auctionAmount\",\n" +
            "        \"expenses\":\"$expenses\"\n" +
            "        }}\n" +
            "    }},\n" +
            "    {\"$unwind\":{\"path\":\"$report\", \"includeArrayIndex\":\"rownum\"}},\n" +
            "    {\"$sort\":{\"report.utc\":1}}    \n" +
            "]";

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
        populateCellStyles(xls);

        if (client == null) {
            client = MongoClient.createShared(rc.vertx(), new JsonObject(), "pawnshop-report");
        }
        xls.createSheet(branchId);
        xls.setActiveSheet(0);
        createHeader(xls, branchId);

        JsonArray pipeline = new JsonArray(String.format(query, branchId, dateFrom, dateTo, "%d/%m/%Y"));
        AggregateOptions aggregateOptions = new AggregateOptions();
        aggregateOptions.setMaxTime(30000);
        aggregateOptions.setBatchSize(10000);
        client.aggregateWithOptions(CrudHandler.SupportedObjectTypes.REPORT.name().toLowerCase(), pipeline, aggregateOptions)
                .toObservable()
                .subscribe(
                        success -> {
                            HSSFRow xlsRow = xls.getSheet(branchId).createRow(xls.getSheet(branchId).getLastRowNum() + 1);
                            xlsRow.createCell(0).setCellValue(String.valueOf(success.getJsonObject("report").getValue("date")));
                            xlsRow.createCell(1).setCellValue(String.valueOf(success.getJsonObject("report").getValue("username")));
                            xlsRow.createCell(2).setCellValue(String.valueOf(success.getJsonObject("report").getValue("loanersAsset")));
                            xlsRow.createCell(3).setCellValue(Double.valueOf(success.getJsonObject("report").getValue("loanersPawned").toString()).longValue());
                            xlsRow.createCell(4).setCellValue(Double.valueOf(success.getJsonObject("report").getValue("loanersBought").toString()).longValue());
                            xlsRow.createCell(5).setCellValue(String.valueOf(success.getJsonObject("report").getValue("pawnersRate")));
                            xlsRow.createCell(6).setCellValue(String.valueOf(success.getJsonObject("report").getValue("pawnersRatePercent")));
                            xlsRow.createCell(7).setCellValue(String.valueOf(success.getJsonObject("report").getValue("cashboxMorning")));
                            xlsRow.createCell(8).setCellValue(String.valueOf(success.getJsonObject("report").getValue("volume")));
                            xlsRow.createCell(9).setCellValue(Double.valueOf(success.getJsonObject("report").getValue("loanedRub").toString()).longValue());
                            xlsRow.createCell(10).setCellValue(Double.valueOf(success.getJsonObject("report").getValue("repayedRub").toString()).longValue());
                            xlsRow.createCell(11).setCellValue(String.valueOf(success.getJsonObject("report").getValue("totalPercentRecieved")));
                            xlsRow.createCell(12).setCellValue(String.valueOf(success.getJsonObject("report").getValue("percentRecieved")));
                            xlsRow.createCell(13).setCellValue(String.valueOf(success.getJsonObject("report").getValue("dailyGrowth")));
                            xlsRow.createCell(14).setCellValue(String.valueOf(success.getJsonObject("report").getValue("dailyGrowthPercent")));
                            xlsRow.createCell(15).setCellValue(String.valueOf(success.getJsonObject("report").getValue("goldBalance")));
                            xlsRow.createCell(16).setCellValue(Double.valueOf(success.getJsonObject("report").getValue("goldBought").toString()));
                            xlsRow.createCell(17).setCellValue(Double.valueOf(success.getJsonObject("report").getValue("goldSold").toString()));
                            xlsRow.createCell(18).setCellValue(String.valueOf(success.getJsonObject("report").getValue("silverBalance")));
                            xlsRow.createCell(19).setCellValue(Double.valueOf(success.getJsonObject("report").getValue("silverBought").toString()));
                            xlsRow.createCell(20).setCellValue(Double.valueOf(success.getJsonObject("report").getValue("silverSold").toString()));
                            xlsRow.createCell(21).setCellValue(String.valueOf(success.getJsonObject("report").getValue("diamondsBalance")));
                            xlsRow.createCell(22).setCellValue(Double.valueOf(success.getJsonObject("report").getValue("diamondsBought").toString()));
                            xlsRow.createCell(23).setCellValue(Double.valueOf(success.getJsonObject("report").getValue("diamondsSold").toString()));
                            xlsRow.createCell(24).setCellValue(String.valueOf(success.getJsonObject("report").getValue("goodsBalance")));
                            xlsRow.createCell(25).setCellValue(Double.valueOf(success.getJsonObject("report").getValue("goodsBought").toString()));
                            xlsRow.createCell(26).setCellValue(Double.valueOf(success.getJsonObject("report").getValue("goodsSold").toString()));
                            xlsRow.createCell(27).setCellValue(Double.valueOf(success.getJsonObject("report").getValue("tradesActive").toString()).longValue());
                            xlsRow.createCell(28).setCellValue(Double.valueOf(success.getJsonObject("report").getValue("silverTradeWeight").toString()));
                            xlsRow.createCell(29).setCellValue(Double.valueOf(success.getJsonObject("report").getValue("goldTradeWeight").toString()));
                            xlsRow.createCell(30).setCellValue(Double.valueOf(success.getJsonObject("report").getValue("metalTradeSum").toString()).longValue());
                            xlsRow.createCell(31).setCellValue(Long.valueOf(success.getJsonObject("report").getValue("goodsTradeSum").toString()));
                            xlsRow.createCell(32).setCellValue(Long.valueOf(success.getJsonObject("report").getValue("auctionAmount").toString()));
                            xlsRow.createCell(33).setCellValue(calculateExpences(success.getJsonObject("report").getValue("expenses")));
                            for (int i = 0; i < 34; i++) {
                                xlsRow.getCell(i).setCellStyle(getDataStyle(i));
                            }
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

    private Long calculateExpences(Object value) {
        if (value == null) {
            return 0L;
        }
        try {
            JsonArray expenses = (JsonArray) value;
            return expenses.stream()
                    .map(j -> {
                        JsonObject e = (JsonObject) j;
                        if (e != null && e.containsKey("sum")) {
                            return Double.valueOf(e.getValue("sum").toString()).longValue();
                        }
                        return 0L;
                    })
                    .reduce(0L, Long::sum);
        } catch (Exception ex) {
            return null;
        }
    }

    private void populateCellStyles(HSSFWorkbook xls) {
        cellStyles.add(createCellStyle(xls, (short) 14, true, HorizontalAlignment.CENTER, (short) 0));      //0
        cellStyles.add(createCellStyle(xls, (short) 12, true, HorizontalAlignment.CENTER, (short) 0));      //1

        cellStyles.add(createCellStyle(xls, (short) 10, true, HorizontalAlignment.CENTER, (short) 0));      //2
        cellStyles.add(createCellStyle(xls, (short) 10, true, HorizontalAlignment.CENTER, (short) 48));     //3
        cellStyles.add(createCellStyle(xls, (short) 10, true, HorizontalAlignment.CENTER, (short) 13));     //4
        cellStyles.add(createCellStyle(xls, (short) 10, true, HorizontalAlignment.CENTER, (short) 51));     //5
        cellStyles.add(createCellStyle(xls, (short) 10, true, HorizontalAlignment.GENERAL, (short) 55));    //6

        cellStyles.add(createCellStyle(xls, (short) 10, false, HorizontalAlignment.GENERAL, (short) 0));    //7
        cellStyles.add(createCellStyle(xls, (short) 10, false, HorizontalAlignment.GENERAL, (short) 48));   //8
        cellStyles.add(createCellStyle(xls, (short) 10, false, HorizontalAlignment.GENERAL, (short) 13));   //9
        cellStyles.add(createCellStyle(xls, (short) 10, false, HorizontalAlignment.GENERAL, (short) 51));   //10
        cellStyles.add(createCellStyle(xls, (short) 10, false, HorizontalAlignment.GENERAL, (short) 55));   //11
        cellStyles.add(createCellStyle(xls, (short) 14, true, HorizontalAlignment.CENTER, (short) 0));      //12
        cellStyles.get(12).setWrapText(true);
        cellStyles.add(createCellStyle(xls, (short) 10, true, HorizontalAlignment.CENTER, (short) 0));      //13
        cellStyles.get(13).setWrapText(true);
    }

    private HSSFCellStyle createCellStyle(HSSFWorkbook xls, short fontSize, boolean isBold, HorizontalAlignment horizontalAlignment, short foregroundColorIndex) {
        HSSFFont font = xls.createFont();
        font.setFontHeightInPoints(fontSize);
        font.setBold(isBold);

        HSSFCellStyle cellStyle = xls.createCellStyle();
        cellStyle.setFont(font);
        cellStyle.setAlignment(horizontalAlignment);
        cellStyle.setVerticalAlignment(VerticalAlignment.CENTER);
        if (foregroundColorIndex != 0) {
            cellStyle.setFillForegroundColor(foregroundColorIndex);
            cellStyle.setFillPattern(FillPatternType.SOLID_FOREGROUND);
        }
        return setBorders(cellStyle);
    }

    private HSSFCellStyle setBorders(HSSFCellStyle style) {
        style.setBorderTop(BorderStyle.THIN);
        style.setBorderBottom(BorderStyle.THIN);
        style.setBorderLeft(BorderStyle.THIN);
        style.setBorderRight(BorderStyle.THIN);
        style.setBottomBorderColor(IndexedColors.BLACK.getIndex());
        style.setTopBorderColor(IndexedColors.BLACK.getIndex());
        style.setLeftBorderColor(IndexedColors.BLACK.getIndex());
        style.setRightBorderColor(IndexedColors.BLACK.getIndex());
        return style;
    }

    private HSSFCellStyle getDataStyle(int columnIndex) {
        short colors[] = {7, 7, 8, 9, 10, 7, 7, 7, 8, 9, 10, 7, 9, 8, 7, 7, 9, 10, 7, 9, 10, 7, 9, 10, 7, 9, 10, 7, 11, 9, 7, 7, 7, 7};
        return cellStyles.get(colors[columnIndex]);
    }

    private void createHeader(HSSFWorkbook xls, String branchName) {
        int[][] styles = {
                {0, 0, 0, 0, 0, 0, 0, 13, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 12, 12},
                {0, 0, 3, 4, 5, 2, 2, 13, 3, 4, 5, 2, 2, 2, 2, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0, 0, 0, 0, 0, 12, 12},
                {0, 0, 3, 4, 5, 2, 2, 13, 3, 4, 5, 2, 2, 2, 2, 2, 4, 5, 2, 4, 5, 2, 4, 5, 2, 4, 5, 1, 1, 1, 2, 0, 12, 12},
                {0, 0, 3, 4, 5, 2, 2, 13, 3, 4, 5, 2, 4, 3, 2, 2, 4, 5, 2, 4, 5, 2, 4, 5, 2, 4, 5, 2, 6, 4, 2, 2, 12, 12}
        };
        HSSFRow row;
        for (int i = 0; i < styles.length; i++) {
            row = xls.getSheet(branchName).createRow(i);
            for (int j = 0; j < styles[i].length; j++) {
                row.createCell(j).setCellStyle(cellStyles.get(styles[i][j]));
            }
        }

        row = xls.getSheet(branchName).getRow(0);
        row.getCell(0).setCellValue("Дата");
        row.getCell(1).setCellValue("Оценщик");
        row.getCell(2).setCellValue("Заёмщики");
        row.getCell(7).setCellValue("Остаток касс на утро");
        row.getCell(8).setCellValue("Корзина");
        row.getCell(27).setCellValue("Торги");
        row.getCell(31).setCellValue("Торги вещи");
        row.getCell(32).setCellValue("Сумма реализации по торгам");
        row.getCell(33).setCellValue("Расходы по отделению");

        //second row
        row = xls.getSheet(branchName).getRow(1);
        row.getCell(2).setCellValue("актив");
        row.getCell(3).setCellValue("взяли");
        row.getCell(4).setCellValue("погасили");
        row.getCell(5).setCellValue("прирост за день");
        row.getCell(8).setCellValue("объем");
        row.getCell(9).setCellValue("выдано");
        row.getCell(10).setCellValue("погашено");
        row.getCell(11).setCellValue("получено % в руб");
        row.getCell(13).setCellValue("прирост за день");
        row.getCell(15).setCellValue("Золото 999,9");
        row.getCell(18).setCellValue("Серебро 999,9");
        row.getCell(21).setCellValue("Бриллианты");
        row.getCell(24).setCellValue("Вещи");

        //third row
        row = xls.getSheet(branchName).getRow(2);
        row.getCell(15).setCellValue("остаток");
        row.getCell(16).setCellValue("принято");
        row.getCell(17).setCellValue("погашено");
        row.getCell(18).setCellValue("остаток");
        row.getCell(19).setCellValue("принято");
        row.getCell(20).setCellValue("погашено");
        row.getCell(21).setCellValue("остаток");
        row.getCell(22).setCellValue("принято");
        row.getCell(23).setCellValue("погашено");
        row.getCell(24).setCellValue("остаток");
        row.getCell(25).setCellValue("принято");
        row.getCell(26).setCellValue("погашено");
        row.getCell(27).setCellValue("актив");
        row.getCell(28).setCellValue("грамм");

        //forth row
        row = xls.getSheet(branchName).getRow(3);
        row.getCell(2).setCellValue("чел.");
        row.getCell(3).setCellValue("чел.");
        row.getCell(4).setCellValue("чел.");
        row.getCell(5).setCellValue("чел.");
        row.getCell(6).setCellValue("%");
        row.getCell(8).setCellValue("рублей");
        row.getCell(9).setCellValue("рублей");
        row.getCell(10).setCellValue("рублей");
        row.getCell(11).setCellValue("всего");
        row.getCell(12).setCellValue("за день");
        row.getCell(13).setCellValue("рублей");
        row.getCell(14).setCellValue("%");
        row.getCell(15).setCellValue("грамм");
        row.getCell(16).setCellValue("грамм");
        row.getCell(17).setCellValue("грамм");
        row.getCell(18).setCellValue("грамм");
        row.getCell(19).setCellValue("грамм");
        row.getCell(20).setCellValue("грамм");
        row.getCell(21).setCellValue("карат");
        row.getCell(22).setCellValue("карат");
        row.getCell(23).setCellValue("карат");
        row.getCell(24).setCellValue("руб");
        row.getCell(25).setCellValue("руб");
        row.getCell(26).setCellValue("руб");
        row.getCell(27).setCellValue("чел.");
        row.getCell(28).setCellValue("ag 999,9");
        row.getCell(29).setCellValue("au 999,9");
        row.getCell(30).setCellValue("рублей");
        row.getCell(31).setCellValue("рублей");


        //merging
        HSSFSheet sheet = xls.getSheet(branchName);
        sheet.addMergedRegion(new CellRangeAddress(0, 3, 0, 0));    // Заемщики
        sheet.addMergedRegion(new CellRangeAddress(0, 3, 1, 1));    // Оценщик
        sheet.addMergedRegion(new CellRangeAddress(0, 0, 2, 6));    // Заемщики
        sheet.addMergedRegion(new CellRangeAddress(1, 2, 2, 2));    // актив
        sheet.addMergedRegion(new CellRangeAddress(1, 2, 3, 3));    // взяли
        sheet.addMergedRegion(new CellRangeAddress(1, 2, 4, 4));    // погасили
        sheet.addMergedRegion(new CellRangeAddress(1, 2, 5, 6));    // прирост за день
        sheet.addMergedRegion(new CellRangeAddress(0, 3, 7, 7));    // остаток касс на утро
        sheet.addMergedRegion(new CellRangeAddress(0, 0, 8, 26));   // корзина
        sheet.addMergedRegion(new CellRangeAddress(1, 2, 8, 8));    // объем
        sheet.addMergedRegion(new CellRangeAddress(1, 2, 9, 9));    // выдано
        sheet.addMergedRegion(new CellRangeAddress(1, 2, 10, 10));  // погашено
        sheet.addMergedRegion(new CellRangeAddress(1, 2, 11, 12));  // получено % в руб
        sheet.addMergedRegion(new CellRangeAddress(1, 2, 13, 14));  // прирост за день
        sheet.addMergedRegion(new CellRangeAddress(1, 1, 15, 17));  // золото
        sheet.addMergedRegion(new CellRangeAddress(1, 1, 18, 20));  // серебро
        sheet.addMergedRegion(new CellRangeAddress(1, 1, 21, 23));  // брилианты
        sheet.addMergedRegion(new CellRangeAddress(1, 1, 24, 26));  // вещи
        sheet.addMergedRegion(new CellRangeAddress(0, 1, 27, 30));  // торги
        sheet.addMergedRegion(new CellRangeAddress(2, 2, 28, 29));  // грамм
        sheet.addMergedRegion(new CellRangeAddress(0, 2, 31, 31));  // торги вещи
        sheet.addMergedRegion(new CellRangeAddress(0, 3, 32, 32));  // сумма реализации по торгам
        sheet.addMergedRegion(new CellRangeAddress(0, 3, 33, 33));  // расход отделения

        int[] columnWidths = {2742, 4388, 2560, 2560, 2560, 2048, 2560, 4242, 2560, 2560, 2560, 2560, 2560, 2560, 2560, 3108, 3108, 3108, 3108, 3108, 3108, 3108, 3108, 3108, 3108, 3108, 3108, 2706, 2742, 2742, 2742, 4754, 4790, 4754};
        for (int i = 0; i < columnWidths.length; i++) {
            sheet.setColumnWidth(i, columnWidths[i]);
        }
    }

    public void showCells() {
        try {
            HSSFWorkbook xls = new HSSFWorkbook(new FileInputStream("report.xls"));
            HSSFSheet sheet = xls.getSheet(xls.getSheetName(0));
            HSSFRow row = sheet.getRow(10);
            for (int i = 0; i < 40; i++) {
                System.out.println(
                        row.getCell(i).getCellStyle().getFillForegroundColorColor().getIndex() + " - " +
                                row.getCell(i).getCellStyle().getFillForegroundColorColor().getIndex2() + " - " +
                                row.getCell(i).getCellStyle().getFillForegroundColorColor().getHexString() + " - " + row.getCell(i).getCellStyle().getFillForegroundColorColor().getTriplet()
                );
            }
        } catch (Exception ex) {
            System.out.println(ex.getMessage());
        }

    }
}
