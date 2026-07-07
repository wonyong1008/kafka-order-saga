package com.example.kafkasaga.inventory;

import com.example.kafkasaga.inventory.domain.ProductStock;
import com.example.kafkasaga.inventory.repository.ProductStockRepository;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

/** 데모용 초기 재고 시드 데이터. P002는 재고 부족 시나리오를 쉽게 재현하기 위해 수량을 적게 잡았다. */
@Component
public class StockDataInitializer implements ApplicationRunner {

    private final ProductStockRepository productStockRepository;

    public StockDataInitializer(ProductStockRepository productStockRepository) {
        this.productStockRepository = productStockRepository;
    }

    @Override
    public void run(ApplicationArguments args) {
        if (productStockRepository.count() > 0) {
            return;
        }
        productStockRepository.save(new ProductStock("P001", 100));
        productStockRepository.save(new ProductStock("P002", 3));
        productStockRepository.save(new ProductStock("P003", 1000));
    }
}
