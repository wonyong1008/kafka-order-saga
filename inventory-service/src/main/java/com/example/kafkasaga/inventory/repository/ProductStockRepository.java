package com.example.kafkasaga.inventory.repository;

import com.example.kafkasaga.inventory.domain.ProductStock;
import org.springframework.data.jpa.repository.JpaRepository;

public interface ProductStockRepository extends JpaRepository<ProductStock, String> {
}
