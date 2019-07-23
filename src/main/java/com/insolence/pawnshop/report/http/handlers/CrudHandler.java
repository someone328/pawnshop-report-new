package com.insolence.pawnshop.report.http.handlers;

import io.vertx.core.Handler;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.reactivex.ext.web.RoutingContext;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CrudHandler implements Handler<RoutingContext> {

  @Override
  public void handle(RoutingContext rc) {
    String objectType = rc.request().getParam("objectType");
    String operationType = rc.request().getParam("operationType");
    Operarions operationTypeEnum;
    
    try {
    	SupportedObjectTypes.valueOf(objectType.toUpperCase());
    } catch (Exception e) {
    	rc.response().setStatusCode(400).end("Unsupported object type " + objectType);
    	return;
    }
    
    try {
    	operationTypeEnum=Operarions.valueOf(operationType.toUpperCase());
    } catch (Exception e){
    	rc.response().setStatusCode(400).end("Unsupported operationType type " + operationType);
    	return;
    }
    
    DeliveryOptions deliveryOptions = new DeliveryOptions().addHeader("objectType", objectType);
    JsonObject jsonBody = rc.getBodyAsJson();
    
    switch (operationTypeEnum) {
    	case PUT -> processPutOperation(rc,deliveryOptions,jsonBody);
    	case GET -> processGetOperation(rc,deliveryOptions,jsonBody);
    	case DELETE -> processDeleteOperation(rc,deliveryOptions,jsonBody);
    	default -> rc.response().setStatusCode(400).end("Unsupported operationType type " + operationType);
    }
    
  }
  
  private void processDeleteOperation(RoutingContext rc,DeliveryOptions deliveryOptions,JsonObject jsonBody){
	  String idToDelete = jsonBody.getString("id");
	  log.info(idToDelete);
	  rc.vertx()
	  	.eventBus()
	  	.rxSend("crud.delete", idToDelete, deliveryOptions)
	  	.map(responce -> responce.body().toString())
	  	.subscribe(success -> rc.response().setStatusCode(200).end(success), 
	  			   error -> {
	  				   log.error("Process DELETE error", error);
	  				   rc.response().setStatusCode(500).end();
	  			   });
}

private void processGetOperation(RoutingContext rc,DeliveryOptions deliveryOptions,JsonObject jsonBody) {
	rc.vertx()
	    .eventBus()
	    .rxSend("crud.get", jsonBody, deliveryOptions)
	    .map(response -> response.body().toString())
	    .subscribe(
	        crudResult ->
	            rc.response()
	                .putHeader("content-type", "application/json; charset=utf-8")
	                .end(crudResult),
	        error -> {
	        	log.error("Crud handler error. ", error);
	        	rc.fail(500);
	        });
  }

  private void processPutOperation(RoutingContext rc,DeliveryOptions deliveryOptions,JsonObject jsonBody){
	rc.vertx()
	    .eventBus()
	    .rxSend("crud.put", jsonBody, deliveryOptions)
	    .map(response -> response.body().toString())
	    .subscribe(
	        crudResult ->
	            rc.response()
	                .putHeader("content-type", "application/json; charset=utf-8")
	                .end(crudResult),
	        error -> {
	        	log.error("Crud handler error. ", error);
	        	rc.fail(500);
	        });
	}

  public boolean isSupportableType(String type) {
    return SupportedObjectTypes.valueOf(type.toUpperCase()).ordinal() != 9999;
  }

  public enum SupportedObjectTypes {
	BRANCH,
	USER,
    REPORT
  }

  public enum Operarions {
    PUT,
    GET,
    DELETE
  }
}
