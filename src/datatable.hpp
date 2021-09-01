#pragma once

#include <iostream>
#include <map>
#include <variant>
#include <string>
#include <vector>
#include <chrono>

#include "rxcpp/rx.hpp"
#include "cxxfaker/include/Internet.h"

typedef std::string Key;
typedef std::variant<int, float, long, std::string> Value;
typedef std::map<Key, Value> Row;
typedef std::vector<Row> Rows;

void showValue(const Value& value) {
    try {
      std::cout << std::get<int>(value) << std::endl ; 
    }
    catch (const std::bad_variant_access& ex) {
    }

    try {
      std::cout << std::get<long>(value) << std::endl ; 
    }
    catch (const std::bad_variant_access& ex) {
    }

    try {
      std::cout << std::get<float>(value) << std::endl ; 
    }
    catch (const std::bad_variant_access& ex) {
    }

    try {
      std::cout << std::get<std::string>(value) << std::endl ; 
    }
    catch (const std::bad_variant_access& ex) {
    }
}

void showRow(const std::map<Key, Value> & row) {
    std::cout << "timestamp : " << std::get<long>(row.at("timestamp"))
        << " usage : " << std::get<int>(row.at("usage"))
        << " host : " << std::get<std::string>(row.at("host")) 
        << " tag : " << std::get<std::string>(row.at("tag")) 
        << std::endl;
}

auto makeDataTableFlow(int size, int interval = 1000)
{
    auto flow = rxcpp::observable<>::create<Row>(
        [=](rxcpp::subscriber<Row> s)
        {
            auto generator = cxxfaker::providers::Internet();
            generator.Seed(123);

            int currentSize = 0;
            while ( size == -1 || currentSize < size ) {
                auto t = std::chrono::system_clock::now();
                long ts = std::chrono::duration_cast<std::chrono::milliseconds>(t.time_since_epoch()).count();
                auto tags = std::vector<std::string>({"A","B","C"});
                auto it = generator.randomElement(tags);
                Row row{
                    {"timestamp", ts},
                    {"usage", generator.randomInt(0,100)},
                    {"host", generator.IPv4()},
                    {"tag", *it++}
                };
                s.on_next(row);
                std::this_thread::sleep_for(std::chrono::milliseconds(interval));
                currentSize ++;
            }
            s.on_completed();
        });
    
    return flow;
}

class DataTable
{
private:
    Rows data;

public:
    DataTable(const Rows &n) : data(n)
    {
    }

    void addValue(const Key &key, const Value &value)
    {
        data.push_back({{key, value}});
    }

    Rows getValue()
    {
        return data;
    }
};