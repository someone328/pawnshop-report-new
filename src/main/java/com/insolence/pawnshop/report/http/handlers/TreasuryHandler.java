package com.insolence.pawnshop.report.http.handlers;

import io.vertx.core.Handler;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.mongo.AggregateOptions;
import io.vertx.reactivex.core.eventbus.EventBus;
import io.vertx.reactivex.ext.mongo.MongoClient;
import io.vertx.reactivex.ext.web.RoutingContext;
import lombok.extern.slf4j.Slf4j;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;

@Slf4j
public class TreasuryHandler implements Handler<RoutingContext> {
    private static final String treasuryRequest =
            "[\n" +
                    "  {\n" +
                    "    \"$match\": {\n" +
                    "      \"date\": {\n" +
                    "        \"$gte\": %s,\n" +
                    "        \"$lt\": %s\n" +
                    "      }\n" +
                    "    }\n" +
                    "  },\n" +
                    "  {\n" +
                    "    \"$project\": {\n" +
                    "      \"_id\": 1,\n" +
                    "      \"branch\": 1,\n" +
                    "      \"loanedRub\": 1,\n" +
                    "      \"repayedRub\": 1,\n" +
                    "      \"cashboxEvening\": 1\n" +
                    "    }\n" +
                    "  },\n" +
                    "  {\n" +
                    "    \"$lookup\": {\n" +
                    "      \"from\": \"branch\",\n" +
                    "      \"let\": {\n" +
                    "        \"id\": \"$branch\",\n" +
                    "        \"repayedRub\": \"$repayedRub\",\n" +
                    "        \"loanedRub\": \"$loanedRub\",\n" +
                    "        \"cashbox\": \"$cashboxEvening\"\n" +
                    "      },\n" +
                    "      \"pipeline\": [\n" +
                    "        {\n" +
                    "          \"$match\": {\n" +
                    "            \"$expr\": {\n" +
                    "              \"$eq\": [\n" +
                    "                \"$_id\",\n" +
                    "                \"$$id\"\n" +
                    "              ]\n" +
                    "            }\n" +
                    "          }\n" +
                    "        },\n" +
                    "        {\n" +
                    "          \"$project\": {\n" +
                    "            \"_id\": 0,\n" +
                    "            \"name\": 1,\n" +
                    "            \"legalParty\": 1,\n" +
                    "            \"loanedToday\": {\n" +
                    "              \"$ifNull\": [\n" +
                    "                \"$$loanedRub\",\n" +
                    "                0\n" +
                    "              ]\n" +
                    "            },\n" +
                    "            \"repayedToday\": {\n" +
                    "              \"$ifNull\": [\n" +
                    "                \"$$repayedRub\",\n" +
                    "                0\n" +
                    "              ]\n" +
                    "            },\n" +
                    "            \"balance\": {\n" +
                    "              \"$subtract\": [\n" +
                    "                {\n" +
                    "                  \"$toDouble\": {\n" +
                    "                    \"$ifNull\": [\n" +
                    "                      \"$$loanedRub\",\n" +
                    "                      0\n" +
                    "                    ]\n" +
                    "                  }\n" +
                    "                },\n" +
                    "                {\n" +
                    "                  \"$toDouble\": {\n" +
                    "                    \"$ifNull\": [\n" +
                    "                      \"$$repayedRub\",\n" +
                    "                      0\n" +
                    "                    ]\n" +
                    "                  }\n" +
                    "                }\n" +
                    "              ]\n" +
                    "            },\n" +
                    "            \"cashbox\": {\n" +
                    "              \"$ifNull\": [\n" +
                    "                \"$$cashbox\",\n" +
                    "                0\n" +
                    "              ]\n" +
                    "            },\n" +
                    "            \"normative\": {\n" +
                    "              \"$ifNull\": [\n" +
                    "                \"$normative\",\n" +
                    "                0\n" +
                    "              ]\n" +
                    "            },\n" +
                    "            \"difference\": {\n" +
                    "              \"$subtract\": [\n" +
                    "                {\n" +
                    "                  \"$toDouble\": {\n" +
                    "                    \"$ifNull\": [\n" +
                    "                      \"$normative\",\n" +
                    "                      0\n" +
                    "                    ]\n" +
                    "                  }\n" +
                    "                },\n" +
                    "                {\n" +
                    "                  \"$toDouble\": {\n" +
                    "                    \"$ifNull\": [\n" +
                    "                      \"$$cashbox\",\n" +
                    "                      0\n" +
                    "                    ]\n" +
                    "                  }\n" +
                    "                }\n" +
                    "              ]\n" +
                    "            }\n" +
                    "          }\n" +
                    "        }\n" +
                    "      ],\n" +
                    "      \"as\": \"data\"\n" +
                    "    }\n" +
                    "  },\n" +
                    "  {\n" +
                    "    \"$unwind\": \"$data\"\n" +
                    "  },\n" +
                    "  {\n" +
                    "    \"$lookup\": {\n" +
                    "      \"from\": \"legalparty\",\n" +
                    "      \"localField\": \"data.legalParty\",\n" +
                    "      \"foreignField\": \"_id\",\n" +
                    "      \"as\": \"legal\"\n" +
                    "    }\n" +
                    "  },\n" +
                    "  {\n" +
                    "    \"$unwind\": \"$legal\"\n" +
                    "  },\n" +
                    "  {\n" +
                    "    \"$project\": {\n" +
                    "      \"_id\": 1,\n" +
                    "      \"branch\": 1,\n" +
                    "      \"name\": \"$data.name\",\n" +
                    "      \"legalParty\": \"$data.legalParty\",\n" +
                    "      \"legalName\": \"$legal.name\",\n" +
                    "      \"loanedToday\": \"$data.loanedToday\",\n" +
                    "      \"repayedToday\": \"$data.repayedToday\",\n" +
                    "      \"balance\": \"$data.balance\",\n" +
                    "      \"cashbox\": \"$data.cashbox\",\n" +
                    "      \"normative\": \"$data.normative\",\n" +
                    "      \"difference\": \"$data.difference\"\n" +
                    "    }\n" +
                    "  },\n" +
                    "  {\n" +
                    "    \"$sort\": {\n" +
                    "      \"name\": 1\n" +
                    "    }\n" +
                    "  },\n" +
                    "  {\n" +
                    "    \"$group\": {\n" +
                    "      \"_id\": \"$legalParty\",\n" +
                    "      \"legalParty\": {\n" +
                    "        \"$first\": \"$legalName\"\n" +
                    "      },\n" +
                    "      \"loanedTodaySum\": {\n" +
                    "        \"$sum\": {\n" +
                    "          \"$toDouble\": \"$loanedToday\"\n" +
                    "        }\n" +
                    "      },\n" +
                    "      \"repayedTodaySum\": {\n" +
                    "        \"$sum\": {\n" +
                    "          \"$toDouble\": \"$repayedToday\"\n" +
                    "        }\n" +
                    "      },\n" +
                    "      \"balanceSum\": {\n" +
                    "        \"$sum\": {\n" +
                    "          \"$toDouble\": \"$balance\"\n" +
                    "        }\n" +
                    "      },\n" +
                    "      \"cashboxSum\": {\n" +
                    "        \"$sum\": {\n" +
                    "          \"$toDouble\": \"$cashbox\"\n" +
                    "        }\n" +
                    "      },\n" +
                    "      \"normativeSum\": {\n" +
                    "        \"$sum\": {\n" +
                    "          \"$toDouble\": \"$normative\"\n" +
                    "        }\n" +
                    "      },\n" +
                    "      \"differenceSum\": {\n" +
                    "        \"$sum\": {\n" +
                    "          \"$toDouble\": \"$difference\"\n" +
                    "        }\n" +
                    "      },\n" +
                    "      \"reports\": {\n" +
                    "        \"$push\": {\n" +
                    "          \"branch\": \"$branch\",\n" +
                    "          \"name\": \"$name\",\n" +
                    "          \"loanedToday\": {\n" +
                    "            \"$toDouble\": \"$loanedToday\"\n" +
                    "          },\n" +
                    "          \"repayedToday\": {\n" +
                    "            \"$toDouble\": \"$repayedToday\"\n" +
                    "          },\n" +
                    "          \"balance\": {\n" +
                    "            \"$toDouble\": \"$balance\"\n" +
                    "          },\n" +
                    "          \"cashbox\": {\n" +
                    "            \"$toDouble\": \"$cashbox\"\n" +
                    "          },\n" +
                    "          \"normative\": {\n" +
                    "            \"$toDouble\": \"$normative\"\n" +
                    "          },\n" +
                    "          \"difference\": {\n" +
                    "            \"$toDouble\": \"$difference\"\n" +
                    "          }\n" +
                    "        }\n" +
                    "      }\n" +
                    "    }\n" +
                    "  },\n" +
                    "  {\n" +
                    "    \"$sort\": {\n" +
                    "      \"legalParty\": 1\n" +
                    "    }\n" +
                    "  },\n" +
                    "  {\n" +
                    "    \"$lookup\": {\n" +
                    "      \"from\": \"treasury\",\n" +
                    "      \"let\": {\n" +
                    "        \"lpID\": \"$_id\"\n" +
                    "      },\n" +
                    "      \"pipeline\": [\n" +
                    "        {\n" +
                    "          \"$match\": {\n" +
                    "            \"$expr\": {\n" +
                    "              \"$and\": [\n" +
                    "                {\n" +
                    "                  \"$eq\": [\n" +
                    "                    \"$$lpID\",\n" +
                    "                    \"$legalPartyId\"\n" +
                    "                  ]\n" +
                    "                },\n" +
                    "                {\n" +
                    "                  \"$gte\": [\n" +
                    "                    \"$date\",\n" +
                    "                    %s\n" +
                    "                  ]\n" +
                    "                },\n" +
                    "                {\n" +
                    "                  \"$lt\": [\n" +
                    "                    \"$date\",\n" +
                    "                    %s\n" +
                    "                  ]\n" +
                    "                }\n" +
                    "              ]\n" +
                    "            }\n" +
                    "          }\n" +
                    "        }\n" +
                    "      ],\n" +
                    "      \"as\": \"treasury\"\n" +
                    "    }\n" +
                    "  },\n" +
                    "  {\n" +
                    "    \"$unwind\": {\n" +
                    "      \"path\": \"$treasury\",\n" +
                    "      \"preserveNullAndEmptyArrays\": true\n" +
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
        LocalDate requestedDate = null;
        try {
            Long timestamp = Long.valueOf(rc.request().getParam("timestamp"));
            requestedDate = Instant.ofEpochMilli(timestamp).atZone(ZoneId.of("UTC")).toLocalDate();
            System.out.println(requestedDate);
        } catch (NumberFormatException e) {
            e.printStackTrace();
        }
        long currentDateStartMillis = requestedDate.atStartOfDay(ZoneId.of("UTC")).toInstant().toEpochMilli();
        long nextDayStartMillis = requestedDate.plusDays(1).atStartOfDay(ZoneId.of("UTC")).toInstant().toEpochMilli();
        System.out.println(currentDateStartMillis);
        System.out.println(nextDayStartMillis);
        JsonArray pipeline = new JsonArray(String.format(treasuryRequest, currentDateStartMillis, nextDayStartMillis, currentDateStartMillis, nextDayStartMillis));
        AggregateOptions aggregateOptions = new AggregateOptions();
        aggregateOptions.setAllowDiskUse(true);
        aggregateOptions.setMaxAwaitTime(0L);
        aggregateOptions.setMaxTime(0L);
        client.aggregateWithOptions(CrudHandler.SupportedObjectTypes.REPORT.name().toLowerCase(), pipeline, aggregateOptions)
                .toObservable()
                .reduce(new JsonArray(), (arr, br) -> arr.add(JsonObject.mapFrom(br)))
                .subscribe(
                        success -> {
                            System.out.println(success);
                            rc.response()
                                    .putHeader(HttpHeaders.CONTENT_TYPE, "application/json; charset=utf-8")
                                    .setChunked(true)
                                    .end(success.encodePrettily());
                        },
                        error -> log.error("Что-то пошло не так во время расчёта отчёта казначейства", error)
                );
    }
}
