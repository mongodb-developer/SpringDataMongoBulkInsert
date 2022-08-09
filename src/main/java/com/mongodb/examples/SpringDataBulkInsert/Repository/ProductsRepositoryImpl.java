package com.mongodb.examples.SpringDataBulkInsert.Repository;

import com.mongodb.WriteConcern;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.result.UpdateResult;
import com.mongodb.examples.SpringDataBulkInsert.Model.Products;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.BulkOperations;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;


@Component
public class ProductsRepositoryImpl implements ProductsRepository {

    private static final Logger LOG = LoggerFactory
            .getLogger(ProductsRepository.class);

    @Autowired
    MongoTemplate mongoTemplate;


    public void updateProductQuantity(String name, int newQuantity) {
        Query query = new Query(Criteria.where("name").is(name));
        Update update = new Update();
        update.set("quantity", newQuantity);

        UpdateResult result = mongoTemplate.updateFirst(query, update, Products.class);

        if(result == null)
            LOG.error("No documents updated");
        else
            LOG.info(result.getModifiedCount() + " document(s) updated..");
    }

    public int bulkInsertProducts(int count) {

        //LOG.info("Dropping collection...");
        //mongoTemplate.dropCollection(Products.class);
        //LOG.info("Dropped!");

        Instant start = Instant.now();
        mongoTemplate.setWriteConcern(WriteConcern.W1.withJournal(true));

        LOG.info("Creating random prods");
        Products [] productList = Products.RandomProducts(count);
        LOG.info("Done creating random prods");
        BulkOperations bulkInsertion = mongoTemplate.bulkOps(BulkOperations.BulkMode.UNORDERED, Products.class);

        for (int i=0; i<productList.length; ++i)
            bulkInsertion.insert(productList[i]);

        BulkWriteResult bulkWriteResult = bulkInsertion.execute();

        LOG.info("Bulk insert of "+bulkWriteResult.getInsertedCount()+" documents completed in "+ Duration.between(start, Instant.now()).toMillis() + " milliseconds");
        return bulkWriteResult.getInsertedCount();
    }
}
