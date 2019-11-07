package com.insolence.pawnshop.report.http.handlers;


import com.insolence.pawnshop.report.domain.Report;
import com.insolence.pawnshop.report.domain.ReportCalculations;
import com.insolence.pawnshop.report.domain.StatisticReportForBranch;
import com.insolence.pawnshop.report.domain.StatisticsReportForBranchRow;
import com.insolence.pawnshop.report.util.Pair;
import io.reactivex.Observable;
import io.reactivex.Single;
import io.reactivex.SingleSource;
import io.vertx.core.Handler;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.reactivex.core.eventbus.EventBus;
import io.vertx.reactivex.ext.mongo.MongoClient;
import io.vertx.reactivex.ext.web.RoutingContext;
import lombok.extern.slf4j.Slf4j;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.LocalDate;
import java.time.YearMonth;
import java.time.ZoneId;
import java.util.Date;

import static com.insolence.pawnshop.report.util.DateUtils.getFirstMomentOfYear;
import static com.insolence.pawnshop.report.util.DateUtils.getLastMomentOfYear;
import static com.insolence.pawnshop.report.verticles.CalculateDynamicsVerticle.DYNAMICS_CALCULATIONS;

@Slf4j
public class StatisticsHandler implements Handler<RoutingContext> {
    private static final String statisticsRequest =
            "[\n" +
                    "  {\n" +
                    "    \"$match\": {\n" +
                    "      \"branch\": {\n" +
                    "        \"$ne\": null\n" +
                    "      },\n" +
                    "      \"date\": {\n" +
                    "        \"$gte\": 1546300800000,\n" +
                    "        \"$lte\": 1577750400000\n" +
                    "      }\n" +
                    "    }\n" +
                    "  },\n" +
                    "  {\n" +
                    "    \"$lookup\": {\n" +
                    "      \"from\": \"report\",\n" +
                    "      \"let\": {\n" +
                    "        \"report_branch\": \"$branch\",\n" +
                    "        \"report_date\": \"$date\"\n" +
                    "      },\n" +
                    "      \"pipeline\": [\n" +
                    "        {\n" +
                    "          \"$match\": {\n" +
                    "            \"$expr\": {\n" +
                    "              \"$and\": [\n" +
                    "                {\"$eq\": [\"$branch\",\"$$report_branch\"]},\n" +
                    "                {\"$lte\": [\"$date\",\"$$report_date\"]}\n" +
                    "              ]\n" +
                    "            }\n" +
                    "          }\n" +
                    "        },\n" +
                    "        {\n" +
                    "          \"$group\": {\n" +
                    "            \"_id\": \"branch\",\n" +
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
                    "            }\n" +
                    "          }\n" +
                    "        },\n" +
                    "        {\n" +
                    "          \"$project\": {\n" +
                    "            \"_id\": 0,\n" +
                    "            \"volume\": 1\n" +
                    "          }\n" +
                    "        }\n" +
                    "      ],\n" +
                    "      \"as\": \"volume\"\n" +
                    "    }\n" +
                    "  },\n" +
                    "  {\n" +
                    "    \"$unwind\": \"$volume\"\n" +
                    "  },\n" +
                    "  {\n" +
                    "    \"$group\": {\n" +
                    "      \"_id\": {\n" +
                    "        \"branch\": \"$branch\",\n" +
                    "        \"month\": {\n" +
                    "          \"$month\": {\n" +
                    "            \"$toDate\": \"$date\"\n" +
                    "          }\n" +
                    "        }\n" +
                    "      },\n" +
                    "      \"monthMinDate\": {\n" +
                    "        \"$min\": \"$date\"\n" +
                    "      },\n" +
                    "      \"monthMaxDate\": {\n" +
                    "        \"$max\": \"$date\"\n" +
                    "      },\n" +
                    "      \"monthLoanRub\": {\n" +
                    "        \"$sum\": {\n" +
                    "          \"$convert\": {\n" +
                    "            \"input\": \"$loanedRub\",\n" +
                    "            \"to\": \"double\",\n" +
                    "            \"onError\": 0,\n" +
                    "            \"onNull\": 0\n" +
                    "          }\n" +
                    "        }\n" +
                    "      },\n" +
                    "      \"monthRepayRub\": {\n" +
                    "        \"$sum\": {\n" +
                    "          \"$convert\": {\n" +
                    "            \"input\": \"$repayedRub\",\n" +
                    "            \"to\": \"double\",\n" +
                    "            \"onError\": 0,\n" +
                    "            \"onNull\": 0\n" +
                    "          }\n" +
                    "        }\n" +
                    "      },\n" +
                    "      \"volume\": {\n" +
                    "        \"$sum\": \"$volume.volume\"\n" +
                    "      },\n" +
                    "      \"monthExpenses\": {\n" +
                    "        \"$sum\": {\n" +
                    "          \"$sum\": {\n" +
                    "            \"$map\": {\n" +
                    "              \"input\": \"$expenses\",\n" +
                    "              \"as\": \"expense\",\n" +
                    "              \"in\": {\n" +
                    "                \"$convert\": {\n" +
                    "                  \"input\": \"$$expense.sum\",\n" +
                    "                  \"to\": \"double\",\n" +
                    "                  \"onError\": 0,\n" +
                    "                  \"onNull\": 0\n" +
                    "                }\n" +
                    "              }\n" +
                    "            }\n" +
                    "          }\n" +
                    "        }\n" +
                    "      }\n" +
                    "    }\n" +
                    "  },\n" +
                    "  {\n" +
                    "    \"$lookup\": {\n" +
                    "      \"from\": \"report\",\n" +
                    "      \"let\": {\n" +
                    "        \"report_branch\": \"$_id.branch\",\n" +
                    "        \"report_date\": \"$monthMinDate\"\n" +
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
                    "            \"cashboxMorning\": {\n" +
                    "              \"$convert\": {\n" +
                    "                \"input\": \"$cashboxEvening\",\n" +
                    "                \"to\": \"double\",\n" +
                    "                \"onError\": 0,\n" +
                    "                \"onNull\": 0\n" +
                    "              }\n" +
                    "            }\n" +
                    "          }\n" +
                    "        }\n" +
                    "      ],\n" +
                    "      \"as\": \"cashboxMorning\"\n" +
                    "    }\n" +
                    "  },\n" +
                    "  {\n" +
                    "    \"$lookup\": {\n" +
                    "      \"from\": \"report\",\n" +
                    "      \"let\": {\n" +
                    "        \"report_branch\": \"$_id.branch\",\n" +
                    "        \"report_date\": \"$monthMaxDate\"\n" +
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
                    "                  \"$eq\": [\n" +
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
                    "            \"cashboxEvening\": {\n" +
                    "              \"$convert\": {\n" +
                    "                \"input\": \"$cashboxEvening\",\n" +
                    "                \"to\": \"double\",\n" +
                    "                \"onError\": 0,\n" +
                    "                \"onNull\": 0\n" +
                    "              }\n" +
                    "            }\n" +
                    "          }\n" +
                    "        }\n" +
                    "      ],\n" +
                    "      \"as\": \"cashboxEvening\"\n" +
                    "    }\n" +
                    "  },\n" +
                    "  {\n" +
                    "    \"$lookup\": {\n" +
                    "      \"from\": \"report\",\n" +
                    "      \"let\": {\n" +
                    "        \"report_branch\": \"$_id.branch\",\n" +
                    "        \"report_date\": \"$monthMaxDate\"\n" +
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
                    "            \"monthTradeBalance\": {\n" +
                    "              \"$sum\": {\n" +
                    "                \"$subtract\": [\n" +
                    "                  {\n" +
                    "                    \"$add\": [\n" +
                    "                      {\n" +
                    "                        \"$sum\": {\n" +
                    "                          \"$convert\": {\n" +
                    "                            \"input\": \"$goldTradeSum\",\n" +
                    "                            \"to\": \"double\",\n" +
                    "                            \"onError\": 0,\n" +
                    "                            \"onNull\": 0\n" +
                    "                          }\n" +
                    "                        }\n" +
                    "                      },\n" +
                    "                      {\n" +
                    "                        \"$sum\": {\n" +
                    "                          \"$convert\": {\n" +
                    "                            \"input\": \"$silverTradeSum\",\n" +
                    "                            \"to\": \"double\",\n" +
                    "                            \"onError\": 0,\n" +
                    "                            \"onNull\": 0\n" +
                    "                          }\n" +
                    "                        }\n" +
                    "                      },\n" +
                    "                      {\n" +
                    "                        \"$sum\": {\n" +
                    "                          \"$convert\": {\n" +
                    "                            \"input\": \"$goodsTradeSum\",\n" +
                    "                            \"to\": \"double\",\n" +
                    "                            \"onError\": 0,\n" +
                    "                            \"onNull\": 0\n" +
                    "                          }\n" +
                    "                        }\n" +
                    "                      }\n" +
                    "                    ]\n" +
                    "                  },\n" +
                    "                  {\n" +
                    "                    \"$convert\": {\n" +
                    "                      \"input\": \"$auctionAmount\",\n" +
                    "                      \"to\": \"double\",\n" +
                    "                      \"onError\": 0,\n" +
                    "                      \"onNull\": 0\n" +
                    "                    }\n" +
                    "                  }\n" +
                    "                ]\n" +
                    "              }\n" +
                    "            },\n" +
                    "            \"endBasket\": {\n" +
                    "              \"$sum\": {\n" +
                    "                \"$subtract\": [\n" +
                    "                  {\n" +
                    "                    \"$subtract\": [\n" +
                    "                      {\n" +
                    "                        \"$convert\": {\n" +
                    "                          \"input\": \"$loanedRub\",\n" +
                    "                          \"to\": \"double\",\n" +
                    "                          \"onError\": 0,\n" +
                    "                          \"onNull\": 0\n" +
                    "                        }\n" +
                    "                      },\n" +
                    "                      {\n" +
                    "                        \"$convert\": {\n" +
                    "                          \"input\": \"$repayedRub\",\n" +
                    "                          \"to\": \"double\",\n" +
                    "                          \"onError\": 0,\n" +
                    "                          \"onNull\": 0\n" +
                    "                        }\n" +
                    "                      }\n" +
                    "                    ]\n" +
                    "                  },\n" +
                    "                  {\n" +
                    "                    \"$add\": [\n" +
                    "                      {\n" +
                    "                        \"$sum\": {\n" +
                    "                          \"$convert\": {\n" +
                    "                            \"input\": \"$goldTradeSum\",\n" +
                    "                            \"to\": \"double\",\n" +
                    "                            \"onError\": 0,\n" +
                    "                            \"onNull\": 0\n" +
                    "                          }\n" +
                    "                        }\n" +
                    "                      },\n" +
                    "                      {\n" +
                    "                        \"$sum\": {\n" +
                    "                          \"$convert\": {\n" +
                    "                            \"input\": \"$silverTradeSum\",\n" +
                    "                            \"to\": \"double\",\n" +
                    "                            \"onError\": 0,\n" +
                    "                            \"onNull\": 0\n" +
                    "                          }\n" +
                    "                        }\n" +
                    "                      },\n" +
                    "                      {\n" +
                    "                        \"$sum\": {\n" +
                    "                          \"$convert\": {\n" +
                    "                            \"input\": \"$goodsTradeSum\",\n" +
                    "                            \"to\": \"double\",\n" +
                    "                            \"onError\": 0,\n" +
                    "                            \"onNull\": 0\n" +
                    "                          }\n" +
                    "                        }\n" +
                    "                      }\n" +
                    "                    ]\n" +
                    "                  }\n" +
                    "                ]\n" +
                    "              }\n" +
                    "            }\n" +
                    "          }\n" +
                    "        },\n" +
                    "        {\n" +
                    "          \"$project\": {\n" +
                    "            \"_id\": 0,\n" +
                    "            \"monthTradeBalance\": 1,\n" +
                    "            \"endBasket\": 1\n" +
                    "          }\n" +
                    "        }\n" +
                    "      ],\n" +
                    "      \"as\": \"monthTradeBalance1\"\n" +
                    "    }\n" +
                    "  },\n" +
                    "  {\n" +
                    "    \"$lookup\": {\n" +
                    "      \"from\": \"report\",\n" +
                    "      \"let\": {\n" +
                    "        \"report_branch\": \"$_id.branch\",\n" +
                    "        \"report_date\": \"$monthMinDate\"\n" +
                    "      },\n" +
                    "      \"pipeline\": [\n" +
                    "        {\"$match\": {\n" +
                    "          \"$expr\": {\n" +
                    "            \"$and\": [\n" +
                    "              {\"$eq\": [\"$branch\",\"$$report_branch\"]},\n" +
                    "              {\"$lt\": [\"$date\",\"$$report_date\"]}\n" +
                    "            ]\n" +
                    "          }\n" +
                    "        }\n" +
                    "        },\n" +
                    "        {\n" +
                    "          \"$group\": {\n" +
                    "            \"_id\": \"branch\",\n" +
                    "            \"startBasket\": {\n" +
                    "              \"$sum\": {\n" +
                    "                \"$subtract\": [\n" +
                    "                  {\n" +
                    "                    \"$subtract\": [\n" +
                    "                      {\n" +
                    "                        \"$convert\": {\n" +
                    "                          \"input\": \"$loanedRub\",\n" +
                    "                          \"to\": \"double\",\n" +
                    "                          \"onError\": 0,\n" +
                    "                          \"onNull\": 0\n" +
                    "                        }\n" +
                    "                      },\n" +
                    "                      {\n" +
                    "                        \"$convert\": {\n" +
                    "                          \"input\": \"$repayedRub\",\n" +
                    "                          \"to\": \"double\",\n" +
                    "                          \"onError\": 0,\n" +
                    "                          \"onNull\": 0\n" +
                    "                        }\n" +
                    "                      }\n" +
                    "                    ]\n" +
                    "                  },\n" +
                    "                  {\n" +
                    "                    \"$add\": [\n" +
                    "                      {\n" +
                    "                        \"$sum\": {\n" +
                    "                          \"$convert\": {\n" +
                    "                            \"input\": \"$goldTradeSum\",\n" +
                    "                            \"to\": \"double\",\n" +
                    "                            \"onError\": 0,\n" +
                    "                            \"onNull\": 0\n" +
                    "                          }\n" +
                    "                        }\n" +
                    "                      },\n" +
                    "                      {\n" +
                    "                        \"$sum\": {\n" +
                    "                          \"$convert\": {\n" +
                    "                            \"input\": \"$silverTradeSum\",\n" +
                    "                            \"to\": \"double\",\n" +
                    "                            \"onError\": 0,\n" +
                    "                            \"onNull\": 0\n" +
                    "                          }\n" +
                    "                        }\n" +
                    "                      },\n" +
                    "                      {\n" +
                    "                        \"$sum\": {\"$convert\": {\"input\": \"$goodsTradeSum\",\"to\": \"double\",\"onError\": 0,\"onNull\": 0}}}]}]}}}},\n" +
                    "        {\n" +
                    "          \"$project\": {\n" +
                    "            \"_id\": 0,\n" +
                    "            \"startBasket\": 1\n" +
                    "          }\n" +
                    "        }\n" +
                    "      ],\n" +
                    "      \"as\": \"startBasket1\"\n" +
                    "    }\n" +
                    "  },\n" +
                    "  {\n" +
                    "    \"$lookup\": {\n" +
                    "      \"from\": \"report\",\n" +
                    "      \"let\": {\n" +
                    "        \"report_branch\": \"$_id.branch\",\n" +
                    "        \"minDate\": \"$monthMinDate\",\n" +
                    "        \"maxDate\": \"$monthMaxDate\"\n" +
                    "      },\n" +
                    "      \"pipeline\": [\n" +
                    "        {\"$match\": {\n" +
                    "          \"$expr\": {\n" +
                    "            \"$and\": [\n" +
                    "              {\"$eq\": [\"$branch\",\"$$report_branch\"]},\n" +
                    "              {\"$lte\": [\"$date\",\"$$maxDate\"]},\n" +
                    "              {\"$gte\": [\"$date\",\"$$minDate\"]}\n" +
                    "            ]\n" +
                    "          }\n" +
                    "        }\n" +
                    "        },\n" +
                    "        {\n" +
                    "          \"$group\": {\n" +
                    "            \"_id\": \"branch\",\n" +
                    "            \"auctionAmount\": {\n" +
                    "              \"$sum\": {\n" +
                    "                \"$convert\": {\n" +
                    "                  \"input\": \"$auctionAmount\",\n" +
                    "                  \"to\": \"double\",\n" +
                    "                  \"onError\": 0,\n" +
                    "                  \"onNull\": 0\n" +
                    "                }\n" +
                    "              }\n" +
                    "            },\n" +
                    "            \"monthTradeSum\": {\n" +
                    "              \"$sum\": {\n" +
                    "                \"$add\": [\n" +
                    "                  {\n" +
                    "                    \"$sum\": {\n" +
                    "                      \"$convert\": {\n" +
                    "                        \"input\": \"$goldTradeSum\",\n" +
                    "                        \"to\": \"double\",\n" +
                    "                        \"onError\": 0,\n" +
                    "                        \"onNull\": 0\n" +
                    "                      }\n" +
                    "                    }\n" +
                    "                  },\n" +
                    "                  {\n" +
                    "                    \"$sum\": {\n" +
                    "                      \"$convert\": {\n" +
                    "                        \"input\": \"$silverTradeSum\",\n" +
                    "                        \"to\": \"double\",\n" +
                    "                        \"onError\": 0,\n" +
                    "                        \"onNull\": 0\n" +
                    "                      }\n" +
                    "                    }\n" +
                    "                  },\n" +
                    "                  {\n" +
                    "                    \"$sum\": {\n" +
                    "                      \"$convert\": {\n" +
                    "                        \"input\": \"$goodsTradeSum\",\n" +
                    "                        \"to\": \"double\",\n" +
                    "                        \"onError\": 0,\n" +
                    "                        \"onNull\": 0\n" +
                    "                      }\n" +
                    "                    }\n" +
                    "                  }\n" +
                    "                ]\n" +
                    "              }\n" +
                    "            },\n" +
                    "            \"monthTradeBalance\": {\n" +
                    "              \"$sum\": {\n" +
                    "                \"$subtract\": [\n" +
                    "                  {\n" +
                    "                    \"$add\": [\n" +
                    "                      {\n" +
                    "                        \"$sum\": {\n" +
                    "                          \"$convert\": {\n" +
                    "                            \"input\": \"$goldTradeSum\",\n" +
                    "                            \"to\": \"double\",\n" +
                    "                            \"onError\": 0,\n" +
                    "                            \"onNull\": 0\n" +
                    "                          }\n" +
                    "                        }\n" +
                    "                      },\n" +
                    "                      {\n" +
                    "                        \"$sum\": {\n" +
                    "                          \"$convert\": {\n" +
                    "                            \"input\": \"$silverTradeSum\",\n" +
                    "                            \"to\": \"double\",\n" +
                    "                            \"onError\": 0,\n" +
                    "                            \"onNull\": 0\n" +
                    "                          }\n" +
                    "                        }\n" +
                    "                      },\n" +
                    "                      {\n" +
                    "                        \"$sum\": {\n" +
                    "                          \"$convert\": {\n" +
                    "                            \"input\": \"$goodsTradeSum\",\n" +
                    "                            \"to\": \"double\",\n" +
                    "                            \"onError\": 0,\n" +
                    "                            \"onNull\": 0\n" +
                    "                          }\n" +
                    "                        }\n" +
                    "                      }\n" +
                    "                    ]\n" +
                    "                  },\n" +
                    "                  {\n" +
                    "                    \"$convert\": {\n" +
                    "                      \"input\": \"$auctionAmount\",\n" +
                    "                      \"to\": \"double\",\n" +
                    "                      \"onError\": 0,\n" +
                    "                      \"onNull\": 0\n" +
                    "                    }\n" +
                    "                  }\n" +
                    "                ]\n" +
                    "              }\n" +
                    "            },\n" +
                    "            \"monthAverageBasket\": {\n" +
                    "              \"$sum\": 1\n" +
                    "            }\n" +
                    "          }\n" +
                    "        },\n" +
                    "        {\n" +
                    "          \"$project\": {\n" +
                    "            \"_id\": 0,\n" +
                    "            \"auctionAmount\": 1,\n" +
                    "            \"monthTradeSum\": 1,\n" +
                    "            \"monthTradeBalance\": 1\n" +
                    "          }\n" +
                    "        }\n" +
                    "      ],\n" +
                    "      \"as\": \"auctionAmount1\"\n" +
                    "    }\n" +
                    "  },\n" +
                    "  {\n" +
                    "    \"$unwind\": {\n" +
                    "      \"path\":\"$cashboxMorning\",\n" +
                    "      \"preserveNullAndEmptyArrays\": true\n" +
                    "    }\n" +
                    "  },\n" +
                    "  {\n" +
                    "    \"$unwind\": {\n" +
                    "      \"path\":\"$cashboxEvening\",\n" +
                    "      \"preserveNullAndEmptyArrays\": true\n" +
                    "    }\n" +
                    "  },\n" +
                    "  {\n" +
                    "    \"$unwind\": {\n" +
                    "      \"path\":\"$monthTradeBalance1\",\n" +
                    "      \"preserveNullAndEmptyArrays\": true\n" +
                    "    }\n" +
                    "  },\n" +
                    "  {\n" +
                    "    \"$unwind\": {\n" +
                    "      \"path\":\"$auctionAmount1\",\n" +
                    "      \"preserveNullAndEmptyArrays\": true\n" +
                    "    }\n" +
                    "  },\n" +
                    "  {\n" +
                    "    \"$unwind\": {\n" +
                    "      \"path\":\"$startBasket1\",\n" +
                    "      \"preserveNullAndEmptyArrays\": true\n" +
                    "    }\n" +
                    "  },\n" +
                    "  {\n" +
                    "    \"$project\": {\n" +
                    "      \"_id\": 0,\n" +
                    "      \"branch\": \"$_id.branch\",\n" +
                    "      \"month\": \"$_id.month\",\n" +
                    "      \"monthMinDate\": 1,\n" +
                    "      \"monthMaxDate\": 1,\n" +
                    "      \"daysInMonth\": {\n" +
                    "        \"$add\": [\n" +
                    "          {\n" +
                    "            \"$dayOfMonth\": {\n" +
                    "              \"$dateFromParts\": {\n" +
                    "                \"year\": {\n" +
                    "                  \"$year\": {\n" +
                    "                    \"$toDate\": \"$monthMinDate\"\n" +
                    "                  }\n" +
                    "                },\n" +
                    "                \"month\": {\n" +
                    "                  \"$add\": [\n" +
                    "                    {\n" +
                    "                      \"$toDouble\": {\n" +
                    "                        \"$month\": {\n" +
                    "                          \"$toDate\": \"$monthMinDate\"\n" +
                    "                        }\n" +
                    "                      }\n" +
                    "                    },\n" +
                    "                    1\n" +
                    "                  ]\n" +
                    "                },\n" +
                    "                \"day\": -1\n" +
                    "              }\n" +
                    "            }\n" +
                    "          },\n" +
                    "          1\n" +
                    "        ]\n" +
                    "      },\n" +
                    "      \"monthAverageBasket\": {\n" +
                    "        \"$divide\": [\n" +
                    "          \"$volume\",\n" +
                    "          {\n" +
                    "            \"$add\": [\n" +
                    "              {\n" +
                    "                \"$dayOfMonth\": {\n" +
                    "                  \"$dateFromParts\": {\n" +
                    "                    \"year\": {\n" +
                    "                      \"$year\": {\n" +
                    "                        \"$toDate\": \"$monthMinDate\"\n" +
                    "                      }\n" +
                    "                    },\n" +
                    "                    \"month\": {\n" +
                    "                      \"$add\": [\n" +
                    "                        {\n" +
                    "                          \"$toDouble\": {\n" +
                    "                            \"$month\": {\n" +
                    "                              \"$toDate\": \"$monthMinDate\"\n" +
                    "                            }\n" +
                    "                          }\n" +
                    "                        },\n" +
                    "                        1\n" +
                    "                      ]\n" +
                    "                    },\n" +
                    "                    \"day\": -1\n" +
                    "                  }\n" +
                    "                }\n" +
                    "              },\n" +
                    "              1\n" +
                    "            ]\n" +
                    "          }\n" +
                    "        ]\n" +
                    "      },\n" +
                    "      \"monthTradeBalance\": \"$monthTradeBalance1.monthTradeBalance\",\n" +
                    "      \"monthTradeSum\": \"$auctionAmount1.monthTradeSum\",\n" +
                    "      \"tradeIncome\": \"$auctionAmount1.auctionAmount\",\n" +
                    "      \"cashboxStartMorning\": {\n" +
                    "        \"$convert\": {\n" +
                    "          \"input\": \"$cashboxMorning.cashboxMorning\",\n" +
                    "          \"to\": \"double\",\n" +
                    "          \"onError\": 0,\n" +
                    "          \"onNull\": 0\n" +
                    "        }\n" +
                    "      },\n" +
                    "      \"cashboxEndMorning\": {\n" +
                    "        \"$convert\": {\n" +
                    "          \"input\": \"$cashboxEvening.cashboxEvening\",\n" +
                    "          \"to\": \"double\",\n" +
                    "          \"onError\": 0,\n" +
                    "          \"onNull\": 0\n" +
                    "        }\n" +
                    "      },\n" +
                    "      \"monthLoanRub\": 1,\n" +
                    "      \"monthRepayRub\": 1,\n" +
                    "      \"startBasket\": \"$startBasket1.startBasket\",\n" +
                    "      \"endBasket\": \"$monthTradeBalance1.endBasket\",\n" +
                    "      \"monthExpenses\": 1\n" +
                    "    }\n" +
                    "  },\n" +
                    "  {\n" +
                    "    \"$sort\": {\n" +
                    "      \"month\": 1\n" +
                    "    }\n" +
                    "  },\n" +
                    "  {\n" +
                    "    \"$group\": {\n" +
                    "      \"_id\": \"$branch\",\n" +
                    "      \"monthlyReports\": {\n" +
                    "        \"$push\": {\n" +
                    "          \"monthLoanRub\": \"$monthLoanRub\",\n" +
                    "          \"monthRepayRub\": \"$monthRepayRub\",\n" +
                    "          \"monthExpenses\": \"$monthExpenses\",\n" +
                    "          \"month\": \"$month\",\n" +
                    "          \"daysInMonth\": \"$daysInMonth\",\n" +
                    "          \"monthAverageBasket\": {\n" +
                    "            \"$round\": [\n" +
                    "              \"$monthAverageBasket\",\n" +
                    "              2\n" +
                    "            ]\n" +
                    "          },\n" +
                    "          \"monthTradeBalance\":\"$monthTradeBalance\",\n" +
                    "          \"monthTradeSum\": \"$monthTradeSum\",\n" +
                    "          \"tradeIncome\": \"$tradeIncome\",\n" +
                    "          \"cashboxStartMorning\": \"$cashboxStartMorning\",\n" +
                    "          \"cashboxEndMorning\": \"$cashboxEndMorning\",\n" +
                    "          \"endBasket\":\"$endBasket\",\n" +
                    "          \"startBasket\":{\"$ifNull\":[\"$startBasket\",0]}\n" +
                    "        }\n" +
                    "      }\n" +
                    "    }\n" +
                    "  },\n" +
                    "  {\n" +
                    "    \"$lookup\": {\n" +
                    "      \"from\": \"branch\",\n" +
                    "      \"localField\": \"_id\",\n" +
                    "      \"foreignField\": \"_id\",\n" +
                    "      \"as\": \"branchInfo\"\n" +
                    "    }\n" +
                    "  },\n" +
                    "  {\"$unwind\":\"$branchInfo\"},\n" +
                    "  {\"$sort\":{\"branchInfo.name\":1}}\n" +
                    "\n" +
                    "]";
    private static final BigDecimal goldBySilverContentDivision = new BigDecimal(999.9).divide(new BigDecimal(585.0), 4, RoundingMode.HALF_UP);
    private EventBus bus;

    private MongoClient client;

    @Override
    public void handle(RoutingContext rc) {
        if (client == null) {
            client = MongoClient.createShared(rc.vertx(), new JsonObject(), "pawnshop-report");
        }
        int year = Integer.valueOf(rc.request().getParam("year"));
        System.out.println(year);
        bus = rc.vertx().eventBus();

        long startOfYear = getFirstMomentOfYear(year);
        long endOfYear = getLastMomentOfYear(year);
        System.out.println(startOfYear);
        System.out.println(endOfYear);
        JsonArray pipeline = new JsonArray(String.format(statisticsRequest, startOfYear, endOfYear));
        client.aggregate(CrudHandler.SupportedObjectTypes.REPORT.name().toLowerCase(), pipeline)
                .toObservable()
                .reduce(new JsonArray(), (arr, br) -> arr.add(JsonObject.mapFrom(br)))
                .subscribe(
                        success -> {
                            rc.response()
                                    .putHeader(HttpHeaders.CONTENT_TYPE, "application/json; charset=utf-8")
                                    .setChunked(true)
                                    .end(success.encodePrettily());
                        },
                        error -> log.error("", error)
                );
//        client.aggregate(CrudHandler.SupportedObjectTypes.REPORT.name().toLowerCase(), pipeline)
//                .toObservable()
//                .map(json -> {
//                    var branchReport = new StatisticReportForBranch();
//                    branchReport.setBranchInfo(json.getJsonObject("branchInfo").mapTo(BranchInfo.class));
//                    return Pair.of(branchReport, json.getJsonArray("reportStatIndex"));
//                })
//                .concatMapSingle(pair ->
//                        Observable.fromIterable(pair.getRight())
//                                .map(e -> (JsonObject) e)
//                                .concatMapSingle(month ->
//                                        Observable.fromIterable(month.getJsonArray("reports"))
//                                                .map(x -> ((JsonObject) x).mapTo(Report.class))
//                                                .reduceWith(StatisticsReportForBranchRow::new, (row, report) -> {
//                                                    JsonObject firstReportInMonth = (JsonObject) Observable.fromIterable(month.getJsonArray("reports")).blockingFirst();
//                                                    row.setMonthNum(month.getInteger("month"));
//                                                    row.setMonthlyVolumeSum(row.getMonthlyVolumeSum().add(noNull(report.getVolume())));
//                                                    row.setMonthTradeSum(row.getMonthTradeSum().add(noNull(report.getAuctionAmount())));
//                                                    row.setCashboxStartMorning(firstReportInMonth.mapTo(Report.class).getCashboxMorning());
//                                                    row.setCashboxEndMorning(report.getCashboxEvening());
//                                                    row.setMonthLoanRub(row.getMonthLoanRub().add(noNull(report.getLoanedRub())));
//                                                    row.setMonthRepayRub(row.getMonthRepayRub().add(noNull(report.getRepayedRub())));
//                                                    row.setMonthExpenses(row.getMonthExpenses().add(noNull(report.getExpensesSum())));
//                                                    //
//                                                    row.setMonthGoldTradeSum(row.getMonthGoldTradeSum().add(noNull(report.getGoldTradeSum())));
//                                                    row.setMonthGoldTradeWeight(row.getMonthGoldTradeWeight().add(noNull(report.getGoldTradeWeight())));
//                                                    row.setMonthSilverTradeSum(row.getMonthSilverTradeSum().add(noNull(report.getSilverTradeSum())));
//                                                    row.setMonthSilverTradeWeight(row.getMonthSilverTradeWeight().add(noNull(report.getSilverTradeWeight())));
//                                                    row.setMonthlyGoodsTradeSum(row.getMonthlyGoodsTradeSum().add(noNull(report.getGoodsTradeSum())));
//                                                    row.setLastReport(report);
//                                                    return row;
//                                                })
//                                )
//                                //don`t move this rows upper. They must be executed after all calculations
//                                .map(this::calculateMonthTradeBalance)
//                                .map(this::calculateTradeIncome)
//                                .concatMapSingle(row -> calculateStartBasket(row, pair.getLeft()))
//                                .concatMapSingle(row -> calculateEndBasket(row, pair.getLeft()))
//                                .concatMapSingle(row -> calculateMonthAverageBasket(row, pair))
//                                .reduceWith(pair::getLeft, (yearReport, monthReport) -> {
//                                    yearReport.getMonthlyReports().add(monthReport);
//                                    return yearReport;
//                                })
//                )
//                .reduce(new JsonArray(), (arr, br) -> arr.add(JsonObject.mapFrom(br)))
//                .subscribe(
//                        success -> rc.response().putHeader(HttpHeaders.CONTENT_TYPE, "application/json; charset=utf-8").end(success.encodePrettily()),
//                        error -> log.error("", error)
//                );
    }

    private SingleSource<? extends StatisticsReportForBranchRow> calculateMonthAverageBasket(StatisticsReportForBranchRow row, Pair<StatisticReportForBranch, JsonArray> pair) {
        Observable<BigDecimal> volumes = Observable.fromIterable(pair.getRight())
                .filter(month -> ((JsonObject) month).getInteger("month") == row.getMonthNum())
                .concatMap(month -> Observable.fromIterable(((JsonObject) month).getJsonArray("reports")))
                .map(x -> ((JsonObject) x).mapTo(Report.class))
                .filter(report -> report.getBranch().equals(pair.getLeft().getBranchInfo().get_id()))
                .map(Report::getBalancedVolume);
        return volumes.startWith(row.getStartBasket())
                .scan(BigDecimal::add)
                .reduceWith(() -> BigDecimal.ZERO, BigDecimal::add)
                .map(summ -> {
                    row.setMonthAverageBasket(summ.subtract(row.getStartBasket())
                            .divide(new BigDecimal(getNumberOfDays(new Date(row.getLastReport().getDate()))), 2, RoundingMode.HALF_UP));
                    return row;
                });
    }

    private int getNumberOfDays(Date date) {
        YearMonth yearMonth = YearMonth.of(date.getYear(), date.getMonth() + 1);
        return yearMonth.lengthOfMonth();
    }

    private StatisticsReportForBranchRow calculateTradeIncome(StatisticsReportForBranchRow row) {
        row.setTradeIncome(row.getMonthTradeBalance().subtract(row.getMonthTradeSum()));
        return row;
    }

    private Single<StatisticsReportForBranchRow> calculateStartBasket(StatisticsReportForBranchRow row, StatisticReportForBranch report) {
        long currentMonthFirstDay = LocalDate.now().withMonth(row.getMonthNum()).withDayOfMonth(1).atStartOfDay().atZone(ZoneId.of("UTC")).toInstant().toEpochMilli();
        return requestCalculationsFor(report, currentMonthFirstDay)
                .map(calcs -> {
                    row.setStartBasket(calcs.getVolume());
                    return row;
                });
    }

    private Single<StatisticsReportForBranchRow> calculateEndBasket(StatisticsReportForBranchRow row, StatisticReportForBranch report) {
        long nextMonthFirstDay = LocalDate.now().withMonth(row.getMonthNum()).plusMonths(1).withDayOfMonth(1).atStartOfDay().atZone(ZoneId.of("UTC")).toInstant().toEpochMilli();
        return requestCalculationsFor(report, nextMonthFirstDay)
                .map(calcs -> {
                    row.setEndBasket(calcs.getVolume());
                    return row;
                });
    }

    private Single<ReportCalculations> requestCalculationsFor(StatisticReportForBranch report, long nextMonthFirstDay) {
        return bus.<String>rxRequest(DYNAMICS_CALCULATIONS, new JsonObject().put("branchId", report.getBranchInfo().get_id()).put("reportDate", nextMonthFirstDay + ""))
                .map(resp -> new JsonObject(resp.body()).mapTo(ReportCalculations.class));
    }

    /*private StatisticsReportForBranchRow calculateMonthAverageBasket(StatisticsReportForBranchRow row) {
        row.setMonthAverageBasket(row.getMonthlyVolumeSum()
                .divide(new BigDecimal(30), 2, RoundingMode.HALF_UP)
        );
        return row;
    }*/

    private StatisticsReportForBranchRow calculateMonthTradeBalance(StatisticsReportForBranchRow row) {
        row.setMonthTradeBalance(
                row.getMonthGoldTradeSum()
                        .add(row.getMonthSilverTradeSum())
                        .add(row.getMonthlyGoodsTradeSum()));
        return row;
    }
}
