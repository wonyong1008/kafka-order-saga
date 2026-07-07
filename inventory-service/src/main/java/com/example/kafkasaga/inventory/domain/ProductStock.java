package com.example.kafkasaga.inventory.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.persistence.Version;

@Entity
@Table(name = "product_stock")
public class ProductStock {

    @Id
    @Column(length = 20)
    private String productId;

    @Column(nullable = false)
    private int availableQuantity;

    /** 동시 주문에 의한 재고 갱신 충돌을 막기 위한 낙관적 락. */
    @Version
    private long version;

    protected ProductStock() {
    }

    public ProductStock(String productId, int availableQuantity) {
        this.productId = productId;
        this.availableQuantity = availableQuantity;
    }

    public boolean hasEnoughStock(int quantity) {
        return availableQuantity >= quantity;
    }

    public void decrease(int quantity) {
        if (!hasEnoughStock(quantity)) {
            throw new IllegalStateException("재고가 부족합니다: " + productId);
        }
        this.availableQuantity -= quantity;
    }

    public void restore(int quantity) {
        this.availableQuantity += quantity;
    }

    public String getProductId() {
        return productId;
    }

    public int getAvailableQuantity() {
        return availableQuantity;
    }
}
