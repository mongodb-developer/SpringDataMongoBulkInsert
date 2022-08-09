package com.mongodb.examples.SpringDataBulkInsert.Repository;

public interface ProductsRepository {
    void updateProductQuantity(String name, int newQty)  ;
    int bulkInsertProducts(int count);
}
