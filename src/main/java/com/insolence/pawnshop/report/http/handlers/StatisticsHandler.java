package com.insolence.pawnshop.report.http.handlers;


import io.vertx.core.Handler;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.reactivex.core.eventbus.EventBus;
import io.vertx.reactivex.ext.mongo.MongoClient;
import io.vertx.reactivex.ext.web.RoutingContext;
import lombok.extern.slf4j.Slf4j;

import static com.insolence.pawnshop.report.util.DateUtils.getFirstMomentOfYear;
import static com.insolence.pawnshop.report.util.DateUtils.getLastMomentOfYear;

@Slf4j
public class StatisticsHandler implements Handler<RoutingContext> {
    private static final String statisticsRequest =
            "[\n" +
                    "  {\n" +
                    "    \"$match\": {\n" +
                    "      \"date\": {\n" +
                    "        \"$gte\": %s,\n" +
                    "        \"$lte\": %s\n" +
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
                    "    \"$unwind\": {\n" +
                    "      \"path\": \"$cashboxMorning\",\n" +
                    "      \"preserveNullAndEmptyArrays\": true\n" +
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
                    "    \"$unwind\": {\n" +
                    "      \"path\": \"$cashboxEvening\",\n" +
                    "      \"preserveNullAndEmptyArrays\": true\n" +
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
                    "            \"_id\": 0,            \n" +
                    "            \"endBasket\": 1\n" +
                    "          }\n" +
                    "        }\n" +
                    "      ],\n" +
                    "      \"as\": \"monthTradeBalance1\"\n" +
                    "    }\n" +
                    "  },\n" +
                    "  {\n" +
                    "    \"$unwind\": {\n" +
                    "      \"path\": \"$monthTradeBalance1\",\n" +
                    "      \"preserveNullAndEmptyArrays\": true\n" +
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
                    "            \"startBasket\": 1\n" +
                    "          }\n" +
                    "        }\n" +
                    "      ],\n" +
                    "      \"as\": \"startBasket1\"\n" +
                    "    }\n" +
                    "  },\n" +
                    "  {\n" +
                    "    \"$unwind\": {\n" +
                    "      \"path\": \"$startBasket1\",\n" +
                    "      \"preserveNullAndEmptyArrays\": true\n" +
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
                    "                    \"$$maxDate\"\n" +
                    "                  ]\n" +
                    "                },\n" +
                    "                {\n" +
                    "                  \"$gte\": [\n" +
                    "                    \"$date\",\n" +
                    "                    \"$$minDate\"\n" +
                    "                  ]\n" +
                    "                }\n" +
                    "              ]\n" +
                    "            }\n" +
                    "          }\n" +
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
                    "            \"monthTradeBalance\": {\n" +
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
                    "      \"path\": \"$auctionAmount1\",\n" +
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
                    "      \"monthTradeBalance\": \"$auctionAmount1.monthTradeBalance\",\n" +
                    "      \"monthTradeSum\": \"$auctionAmount1.auctionAmount\",\n" +
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
                    "          \"monthTradeBalance\": \"$monthTradeBalance\",\n" +
                    "          \"monthTradeSum\": \"$monthTradeSum\",\n" +
                    "          \"tradeIncome\": \"$tradeIncome\",\n" +
                    "          \"cashboxStartMorning\": \"$cashboxStartMorning\",\n" +
                    "          \"cashboxEndMorning\": \"$cashboxEndMorning\",\n" +
                    "          \"endBasket\": \"$endBasket\",\n" +
                    "          \"startBasket\": {\n" +
                    "            \"$ifNull\": [\n" +
                    "              \"$startBasket\",\n" +
                    "              0\n" +
                    "            ]\n" +
                    "          }\n" +
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
                    "  {\n" +
                    "    \"$unwind\": \"$branchInfo\"\n" +
                    "  },\n" +
                    "  {\n" +
                    "    \"$sort\": {\n" +
                    "      \"branchInfo.name\": 1\n" +
                    "    }\n" +
                    "  }\n" +
                    "]";
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
                        success -> rc.response()
                                .putHeader(HttpHeaders.CONTENT_TYPE, "application/json; charset=utf-8")
                                .setChunked(true)
                                .end(success.encodePrettily()),
                        error -> log.error("Что-то пошло не так во время расчёта статистики", error)
                );
    }
}
