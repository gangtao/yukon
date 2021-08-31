#pragma once

#include <iostream>
#include <map>
#include <variant>
#include <string>
#include <vector>

#include <iostream>

typedef std::map<std::string, std::variant<int, float, std::string>> Row;
typedef std::vector<Row> Rows;

void showValue(const std::variant<int, float, std::string>& value) {
    try {
      std::cout << std::get<int>(value) << std::endl ; 
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

class DataTable {
    private:
        Rows data;
    public:
        DataTable(const Rows& n):data(n) {

        }

        void addValue(const std::string & key, const std::variant<int, float, std::string> & value) { 
            data.push_back({{key, value}});
        }

        Rows getValue() {
            return data;
        }
};