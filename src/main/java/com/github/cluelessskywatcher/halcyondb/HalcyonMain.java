package com.github.cluelessskywatcher.halcyondb;

public class HalcyonMain {
    public static void main(String[] args) {
        SchemaCatalog catalog = HalcyonDBInstance.getCatalog();
        catalog.loadFromFile("sampleSchema.catalog");
    }
}
