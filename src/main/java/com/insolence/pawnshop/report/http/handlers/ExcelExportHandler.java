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
import org.apache.poi.ss.usermodel.*;
import org.apache.poi.ss.util.CellRangeAddress;

import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.List;

@Slf4j
public class ExcelExportHandler implements Handler<RoutingContext> {
    private MongoClient client;
    private List<HSSFCellStyle> cellStyles = new ArrayList<>();
    String query = "[\n" +
            "  {\n" +
            "    \"$match\": {\n" +
            "      \"branch\": \"%s\",\n" +
            "      \"date\": {\n" +
            "        \"$gte\": %s,\n" +
            "        \"$lte\": %s\n" +
            "      }\n" +
            "    }\n" +
            "  },\n" +
            "  {\n" +
            "    \"$lookup\": {\n" +
            "      \"from\": \"user\",\n" +
            "      \"localField\": \"user\",\n" +
            "      \"foreignField\": \"_id\",\n" +
            "      \"as\": \"username\"\n" +
            "    }\n" +
            "  },\n" +
            "  {\n" +
            "    \"$unwind\": {\n" +
            "      \"path\": \"$username\"\n" +
            "    }\n" +
            "  },\n" +
            "  {\n" +
            "    \"$group\": {\n" +
            "      \"_id\": \"$_id\",\n" +
            "      \"report\": {\n" +
            "        \"$push\": {\n" +
            "          \"branch\": \"$branch\",\n" +
            "          \"utc\": \"$date\",\n" +
            "          \"date\": {\n" +
            "            \"$dateToString\": {\n" +
            "              \"date\": {\n" +
            "                \"$toDate\": \"$date\"\n" +
            "              },\n" +
            "              \"format\": \"%s\"\n" +
            "            }\n" +
            "          },\n" +
            "          \"username\": \"$username.name\",\n" +
            "          \"loanersPawned\": \"$loanersPawned\",\n" +
            "          \"loanersBought\": \"$loanersBought\",\n" +
            "          \"pawnersRate\": {\n" +
            "            \"$subtract\": [\n" +
            "              {\n" +
            "                \"$toDouble\": \"$loanersPawned\"\n" +
            "              },\n" +
            "              {\n" +
            "                \"$toDouble\": \"$loanersBought\"\n" +
            "              }\n" +
            "            ]\n" +
            "          },\n" +
            "          \"cashboxEvening\": \"$cashboxEvening\",\n" +
            "          \"loanedRub\": \"$loanedRub\",\n" +
            "          \"repayedRub\": \"$repayedRub\",\n" +
            "          \"percentRecieved\": \"$percentRecieved\",\n" +
            "          \"dailyGrowth\": {\n" +
            "            \"$subtract\": [\n" +
            "              {\n" +
            "                \"$toDouble\": \"$loanedRub\"\n" +
            "              },\n" +
            "              {\n" +
            "                \"$toDouble\": \"$repayedRub\"\n" +
            "              }\n" +
            "            ]\n" +
            "          },\n" +
            "          \"goldBought\": \"$goldBought\",\n" +
            "          \"goldSold\": \"$goldSold\",\n" +
            "          \"silverBought\": \"$silverBought\",\n" +
            "          \"silverSold\": \"$silverSold\",\n" +
            "          \"diamondsBought\": \"$diamondBought\",\n" +
            "          \"diamondsSold\": \"$diamondSold\",\n" +
            "          \"goodsBought\": \"$goodsBought\",\n" +
            "          \"goodsSold\": \"$goodsSold\",\n" +
            "          \"tradesActive\": \"$tradesActive\",\n" +
            "          \"silverTradeWeight\": \"$silverTradeWeight\",\n" +
            "          \"goldTradeWeight\": \"$goldTradeWeight\",\n" +
            "          \"metalTradeSum\": {\n" +
            "            \"$add\": [\n" +
            "              {\n" +
            "                \"$toDouble\": \"$goldTradeSum\"\n" +
            "              },\n" +
            "              {\n" +
            "                \"$toDouble\": \"$silverTradeSum\"\n" +
            "              }\n" +
            "            ]\n" +
            "          },\n" +
            "          \"goodsTradeSum\": \"$goodsTradeSum\",\n" +
            "          \"auctionAmount\": \"$auctionAmount\",\n" +
            "          \"expenses\": \"$expenses\"\n" +
            "        }\n" +
            "      }\n" +
            "    }\n" +
            "  },\n" +
            "  {\n" +
            "    \"$unwind\": {\n" +
            "      \"path\": \"$report\"\n" +
            "    }\n" +
            "  },\n" +
            "  {\n" +
            "    \"$lookup\": {\n" +
            "      \"from\": \"report\",\n" +
            "      \"let\": {\n" +
            "        \"report_branch\": \"$report.branch\",\n" +
            "        \"report_date\": \"$report.utc\"\n" +
            "      },\n" +
            "      \"pipeline\": [\n" +
            "        {\n" +
            "          \"$match\": {\n" +
            "            \"$expr\": {\n" +
            "              \"$and\": [\n" +
            "                {\n" +
            "                  \"$eq\": [\n" +
            "                    \"$branch\",\n" +
            "                    \"$$report_branch\"\n" +
            "                  ]\n" +
            "                },\n" +
            "                {\n" +
            "                  \"$lt\": [\n" +
            "                    \"$date\",\n" +
            "                    \"$$report_date\"\n" +
            "                  ]\n" +
            "                }\n" +
            "              ]\n" +
            "            }\n" +
            "          }\n" +
            "        },\n" +
            "        {\n" +
            "          \"$sort\": {\n" +
            "            \"date\": -1\n" +
            "          }\n" +
            "        },\n" +
            "        {\n" +
            "          \"$limit\": 1\n" +
            "        },\n" +
            "        {\n" +
            "          \"$project\": {\n" +
            "            \"_id\": 0,\n" +
            "            \"value\": \"$cashboxEvening\"\n" +
            "          }\n" +
            "        }\n" +
            "      ],\n" +
            "      \"as\": \"report.cashboxMorning\"\n" +
            "    }\n" +
            "  },\n" +
            "  {\n" +
            "    \"$unwind\": {\n" +
            "      \"path\": \"$report.cashboxMorning\",\n" +
            "      \"preserveNullAndEmptyArrays\": true\n" +
            "    }\n" +
            "  },\n" +
            "  {\n" +
            "    \"$lookup\": {\n" +
            "      \"from\": \"report\",\n" +
            "      \"let\": {\n" +
            "        \"report_branch\": \"$report.branch\",\n" +
            "        \"report_date\": \"$report.utc\"\n" +
            "      },\n" +
            "      \"pipeline\": [\n" +
            "        {\n" +
            "          \"$match\": {\n" +
            "            \"$expr\": {\n" +
            "              \"$and\": [\n" +
            "                {\n" +
            "                  \"$eq\": [\n" +
            "                    \"$branch\",\n" +
            "                    \"$$report_branch\"\n" +
            "                  ]\n" +
            "                },\n" +
            "                {\n" +
            "                  \"$lt\": [\n" +
            "                    \"$date\",\n" +
            "                    \"$$report_date\"\n" +
            "                  ]\n" +
            "                }\n" +
            "              ]\n" +
            "            }\n" +
            "          }\n" +
            "        },\n" +
            "        {\n" +
            "          \"$group\": {\n" +
            "            \"_id\": \"branch\",\n" +
            "            \"loanersAsset\": {\n" +
            "              \"$sum\": {\n" +
            "                \"$subtract\": [\n" +
            "                  {\n" +
            "                    \"$toDouble\": \"$loanersPawned\"\n" +
            "                  },\n" +
            "                  {\n" +
            "                    \"$toDouble\": \"$loanersBought\"\n" +
            "                  }\n" +
            "                ]\n" +
            "              }\n" +
            "            }\n" +
            "          }\n" +
            "        },\n" +
            "        {\n" +
            "          \"$project\": {\n" +
            "            \"_id\": 0,\n" +
            "            \"value\": \"$loanersAsset\"\n" +
            "          }\n" +
            "        }\n" +
            "      ],\n" +
            "      \"as\": \"report.loanersAsset\"\n" +
            "    }\n" +
            "  },\n" +
            "  {\n" +
            "    \"$unwind\": {\n" +
            "      \"path\": \"$report.loanersAsset\",\n" +
            "      \"preserveNullAndEmptyArrays\": true\n" +
            "    }\n" +
            "  },\n" +
            "  {\n" +
            "    \"$lookup\": {\n" +
            "      \"from\": \"report\",\n" +
            "      \"let\": {\n" +
            "        \"report_branch\": \"$report.branch\",\n" +
            "        \"report_date\": \"$report.utc\"\n" +
            "      },\n" +
            "      \"pipeline\": [\n" +
            "        {\n" +
            "          \"$match\": {\n" +
            "            \"$expr\": {\n" +
            "              \"$and\": [\n" +
            "                {\n" +
            "                  \"$eq\": [\n" +
            "                    \"$branch\",\n" +
            "                    \"$$report_branch\"\n" +
            "                  ]\n" +
            "                },\n" +
            "                {\n" +
            "                  \"$lte\": [\n" +
            "                    \"$date\",\n" +
            "                    \"$$report_date\"\n" +
            "                  ]\n" +
            "                }\n" +
            "              ]\n" +
            "            }\n" +
            "          }\n" +
            "        },\n" +
            "        {\n" +
            "          \"$group\": {\n" +
            "            \"_id\": \"branch\",\n" +
            "            \"goldBalance\": {\n" +
            "              \"$sum\": {\n" +
            "                \"$subtract\": [\n" +
            "                  {\n" +
            "                    \"$subtract\": [\n" +
            "                      {\n" +
            "                        \"$toDouble\": \"$goldBought\"\n" +
            "                      },\n" +
            "                      {\n" +
            "                        \"$toDouble\": \"$goldSold\"\n" +
            "                      }\n" +
            "                    ]\n" +
            "                  },\n" +
            "                  {\n" +
            "                    \"$toDouble\": \"$goldTradeWeight\"\n" +
            "                  }\n" +
            "                ]\n" +
            "              }\n" +
            "            },\n" +
            "            \"silverBalance\": {\n" +
            "              \"$sum\": {\n" +
            "                \"$subtract\": [\n" +
            "                  {\n" +
            "                    \"$subtract\": [\n" +
            "                      {\n" +
            "                        \"$toDouble\": \"$silverBought\"\n" +
            "                      },\n" +
            "                      {\n" +
            "                        \"$toDouble\": \"$silverSold\"\n" +
            "                      }\n" +
            "                    ]\n" +
            "                  },\n" +
            "                  {\n" +
            "                    \"$toDouble\": \"$silverTradeWeight\"\n" +
            "                  }\n" +
            "                ]\n" +
            "              }\n" +
            "            },\n" +
            "            \"diamondsBalance\": {\n" +
            "              \"$sum\": {\n" +
            "                \"$subtract\": [\n" +
            "                  {\n" +
            "                    \"$toDouble\": \"$diamondBought\"\n" +
            "                  },\n" +
            "                  {\n" +
            "                    \"$toDouble\": \"$diamondSold\"\n" +
            "                  }\n" +
            "                ]\n" +
            "              }\n" +
            "            },\n" +
            "            \"goodsBalance\": {\n" +
            "              \"$sum\": {\n" +
            "                \"$subtract\": [\n" +
            "                  {\n" +
            "                    \"$subtract\": [\n" +
            "                      {\n" +
            "                        \"$toDouble\": \"$goodsBought\"\n" +
            "                      },\n" +
            "                      {\n" +
            "                        \"$toDouble\": \"$goodsSold\"\n" +
            "                      }\n" +
            "                    ]\n" +
            "                  },\n" +
            "                  {\n" +
            "                    \"$toDouble\": \"$goodsTradeSum\"\n" +
            "                  }\n" +
            "                ]\n" +
            "              }\n" +
            "            },\n" +
            "            \"volume\": {\n" +
            "              \"$sum\": {\n" +
            "                \"$subtract\": [\n" +
            "                  {\n" +
            "                    \"$subtract\": [\n" +
            "                      {\n" +
            "                        \"$toDouble\": {\n" +
            "                          \"$subtract\": [\n" +
            "                            {\n" +
            "                              \"$toDouble\": \"$loanedRub\"\n" +
            "                            },\n" +
            "                            {\n" +
            "                              \"$toDouble\": \"$repayedRub\"\n" +
            "                            }\n" +
            "                          ]\n" +
            "                        }\n" +
            "                      },\n" +
            "                      {\n" +
            "                        \"$toDouble\": \"$goodsTradeSum\"\n" +
            "                      }\n" +
            "                    ]\n" +
            "                  },\n" +
            "                  {\n" +
            "                    \"$toDouble\": {\n" +
            "                      \"$add\": [\n" +
            "                        {\n" +
            "                          \"$toDouble\": \"$goldTradeSum\"\n" +
            "                        },\n" +
            "                        {\n" +
            "                          \"$toDouble\": \"$silverTradeSum\"\n" +
            "                        }\n" +
            "                      ]\n" +
            "                    }\n" +
            "                  }\n" +
            "                ]\n" +
            "              }\n" +
            "            },\n" +
            "            \"totalPercentRecieved\": {\n" +
            "              \"$sum\": {\n" +
            "                \"$toDouble\": \"$percentRecieved\"\n" +
            "              }\n" +
            "            }\n" +
            "          }\n" +
            "        },\n" +
            "        {\n" +
            "          \"$project\": {\n" +
            "            \"_id\": 0,\n" +
            "            \"goldBalance\": 1,\n" +
            "            \"silverBalance\": 1,\n" +
            "            \"diamondsBalance\": 1,\n" +
            "            \"goodsBalance\": 1,\n" +
            "            \"volume\": 1,\n" +
            "            \"totalPercentRecieved\": 1\n" +
            "          }\n" +
            "        }\n" +
            "      ],\n" +
            "      \"as\": \"report.balances\"\n" +
            "    }\n" +
            "  },\n" +
            "  {\n" +
            "    \"$unwind\": {\n" +
            "      \"path\": \"$report.balances\",\n" +
            "      \"preserveNullAndEmptyArrays\": true\n" +
            "    }\n" +
            "  },\n" +
            "  {\n" +
            "    \"$lookup\": {\n" +
            "      \"from\": \"branch\",\n" +
            "      \"localField\": \"report.branch\",\n" +
            "      \"foreignField\": \"_id\",\n" +
            "      \"as\": \"report.branch_info\"\n" +
            "    }\n" +
            "  },\n" +
            "  {\n" +
            "    \"$unwind\": \"$report.branch_info\"\n" +
            "  },\n" +
            "  {\n" +
            "    \"$sort\": {\n" +
            "      \"report.utc\": 1\n" +
            "    }\n" +
            "  }\n" +
            "]\n";
    String branchName = "";

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

        if (client == null) {
            client = MongoClient.createShared(rc.vertx(), new JsonObject(), "pawnshop-report");
        }
        xls.createSheet(branchId);
        xls.setActiveSheet(0);
        populateCellStyles(xls);
        createHeader(xls, branchId);

        JsonArray pipeline = new JsonArray(String.format(query, branchId, dateFrom, dateTo, "%d/%m/%Y"));
        System.out.println(pipeline);
        AggregateOptions aggregateOptions = new AggregateOptions();
        aggregateOptions.setMaxTime(30000);
        aggregateOptions.setBatchSize(10000);
        client.aggregateWithOptions(CrudHandler.SupportedObjectTypes.REPORT.name().toLowerCase(), pipeline, aggregateOptions)
                .toObservable()
                .subscribe(
                        excelData -> {
                            HSSFRow xlsRow = xls.getSheet(branchId).createRow(xls.getSheet(branchId).getLastRowNum() + 1);
                            JsonObject report = excelData.getJsonObject("report");
                            xlsRow.createCell(0).setCellValue(getStringValue(report, "date"));
                            xlsRow.createCell(1).setCellValue(getStringValue(report, "username"));
                            xlsRow.createCell(2).setCellValue(getLongValue(report, "loanersAsset.value"));
                            xlsRow.createCell(3).setCellValue(getLongValue(report, "loanersPawned"));
                            xlsRow.createCell(4).setCellValue(getLongValue(report, "loanersBought"));
                            xlsRow.createCell(5).setCellValue(getLongValue(report, "pawnersRate"));

                            xlsRow.createCell(7).setCellValue(getLongValue(report, "cashboxMorning.value"));
                            xlsRow.createCell(8).setCellValue(getLongValue(report, "balances.volume"));
                            xlsRow.createCell(9).setCellValue(getLongValue(report, "loanedRub"));
                            xlsRow.createCell(10).setCellValue(getLongValue(report, "repayedRub"));
                            xlsRow.createCell(11).setCellValue(getLongValue(report, "balances.totalPercentRecieved"));
                            xlsRow.createCell(12).setCellValue(getLongValue(report, "percentRecieved"));
                            xlsRow.createCell(13).setCellValue(getLongValue(report, "dailyGrowth"));

                            xlsRow.createCell(15).setCellValue(getDoubleValue(report, "balances.goldBalance"));
                            xlsRow.createCell(16).setCellValue(getDoubleValue(report, "goldBought"));
                            xlsRow.createCell(17).setCellValue(getDoubleValue(report, "goldSold"));
                            xlsRow.createCell(18).setCellValue(getDoubleValue(report, "balances.silverBalance"));
                            xlsRow.createCell(19).setCellValue(getDoubleValue(report, "silverBought"));
                            xlsRow.createCell(20).setCellValue(getDoubleValue(report, "silverSold"));
                            xlsRow.createCell(21).setCellValue(getDoubleValue(report, "balances.diamondsBalance"));
                            xlsRow.createCell(22).setCellValue(getDoubleValue(report, "diamondsBought"));
                            xlsRow.createCell(23).setCellValue(getDoubleValue(report, "diamondsSold"));
                            xlsRow.createCell(24).setCellValue(getDoubleValue(report, "balances.goodsBalance"));
                            xlsRow.createCell(25).setCellValue(getDoubleValue(report, "goodsBought"));
                            xlsRow.createCell(26).setCellValue(getDoubleValue(report, "goodsSold"));
                            xlsRow.createCell(27).setCellValue(getLongValue(report, "tradesActive"));
                            xlsRow.createCell(28).setCellValue(getDoubleValue(report, "silverTradeWeight"));
                            xlsRow.createCell(29).setCellValue(getDoubleValue(report, "goldTradeWeight"));
                            xlsRow.createCell(30).setCellValue(getLongValue(report, "metalTradeSum"));
                            xlsRow.createCell(31).setCellValue(getLongValue(report, "goodsTradeSum"));
                            xlsRow.createCell(32).setCellValue(getLongValue(report, "auctionAmount"));
                            xlsRow.createCell(33).setCellValue(calculateExpences(report));
                            if (xlsRow.getRowNum() == 4) {
                                xlsRow.createCell(6).setCellValue("");
                                xlsRow.createCell(14).setCellValue(0);
                            } else {
                                xlsRow.createCell(6).setCellFormula(String.format("F%s/C%s*1", xlsRow.getRowNum(), xlsRow.getRowNum() + 1));
                                xlsRow.createCell(14).setCellFormula(String.format("J%s/I%s", xlsRow.getRowNum() + 1, xlsRow.getRowNum()));
                            }
                            for (int i = 0; i < 34; i++) {
                                xlsRow.getCell(i).setCellStyle(getDataStyle(i));
                            }
                            setSheetName(report.getJsonObject("branch_info").getString("name"));
                        },
                        error -> {
                            log.error("Excel export error.", error);
                            rc.fail(500, new Exception("Ошибка экспорта в excel"));
                        },
                        () -> {
                            HSSFFormulaEvaluator.evaluateAllFormulaCells(xls);
                            ByteArrayOutputStream baos = new ByteArrayOutputStream();
                            xls.setSheetName(0, branchName);
                            xls.write(baos);
                            rc.response()
                                    .putHeader("Content-Type", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet; charset=UTF-8")
                                    .putHeader("Content-Disposition", "attachment; filename=\"report.xlsx\"")
                                    .end(Buffer.buffer(baos.toByteArray()));
                        }
                );
    }

    private String getStringValue(JsonObject rpt, String path) {
        String[] pathParts = path.split("\\.");
        JsonObject tmp = rpt;
        int i;
        for (i = 0; i < pathParts.length - 1; i++) {
            if (tmp.containsKey(pathParts[i])) {
                tmp = tmp.getJsonObject(pathParts[i]);
            } else {
                return "";
            }
        }
        if (tmp.containsKey(pathParts[i])) {
            return String.valueOf(tmp.getValue(pathParts[i]).toString());
        } else {
            return "";
        }
    }

    private Long getLongValue(JsonObject rpt, String path) {
        return getDoubleValue(rpt, path).longValue();
    }

    private Double getDoubleValue(JsonObject rpt, String path) {
        String stringValue = getStringValue(rpt, path);
        return Double.valueOf(stringValue.isEmpty() ? "0" : stringValue);
    }

    private void setSheetName(String name) {
        branchName = name;
    }

    private Long calculateExpences(Object value) {
        if (value == null) {
            return 0L;
        }
        try {
            if (((JsonObject) value).containsKey("expenses")) {
                JsonArray expenses = ((JsonObject) value).getJsonArray("expenses");
                return expenses.stream()
                        .map(j -> {
                            JsonObject e = (JsonObject) j;
                            if (e != null && e.containsKey("sum")) {
                                return Double.valueOf(e.getValue("sum").toString()).longValue();
                            }
                            return 0L;
                        })
                        .reduce(0L, Long::sum);
            } else {
                return 0L;
            }
        } catch (Exception ex) {
            return 0L;
        }
    }

    private void populateCellStyles(HSSFWorkbook xls) {
        if (cellStyles != null && !cellStyles.isEmpty()) {
            cellStyles.clear();
        }
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
        cellStyles.add(createCellStyle(xls, (short) 10, false, HorizontalAlignment.GENERAL, (short) 0));    //14
        cellStyles.get(14).setDataFormat((short) 10);
    }

    private HSSFCellStyle createCellStyle(HSSFWorkbook xls, short fontSize, boolean isBold, HorizontalAlignment
            horizontalAlignment, short foregroundColorIndex) {
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
        short colors[] = {7, 7, 8, 9, 10, 7, 14, 7, 8, 9, 10, 7, 9, 8, 14, 7, 9, 10, 7, 9, 10, 7, 9, 10, 7, 9, 10, 7, 11, 9, 7, 7, 7, 7};
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
