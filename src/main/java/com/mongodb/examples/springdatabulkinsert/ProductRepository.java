package com.mongodb.examples.springdatabulkinsert;

import com.mongodb.WriteConcern;
import com.mongodb.bulk.BulkWriteResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.BulkOperations;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;

@Component
public class ProductRepository {

    private static final Logger LOG = LoggerFactory
            .getLogger(ProductRepository.class);

    @Autowired
    MongoTemplate mongoTemplate;

    public int bulkInsertProducts(int count) {

        LOG.info("Dropping collection...");
        mongoTemplate.dropCollection(Products.class);
        LOG.info("Dropped!");

        Instant start = Instant.now();
        mongoTemplate.setWriteConcern(WriteConcern.W1.withJournal(true));

        Products [] productList = Products.RandomProducts(count);
        BulkOperations bulkInsertion = mongoTemplate.bulkOps(BulkOperations.BulkMode.UNORDERED, Products.class);

        for (int i=0; i<productList.length; ++i)
            bulkInsertion.insert(productList[i]);

        BulkWriteResult bulkWriteResult = bulkInsertion.execute();

        LOG.info("Bulk insert of "+bulkWriteResult.getInsertedCount()+" documents completed in "+ Duration.between(start, Instant.now()).toMillis() + " milliseconds");
        return bulkWriteResult.getInsertedCount();
    }
}