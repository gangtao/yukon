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

void showValue(const Value &value)
{
    try
    {
        std::cout << std::get<int>(value) << std::endl;
    }
    catch (const std::bad_variant_access &ex)
    {
    }

    try
    {
        std::cout << std::get<long>(value) << std::endl;
    }
    catch (const std::bad_variant_access &ex)
    {
    }

    try
    {
        std::cout << std::get<float>(value) << std::endl;
    }
    catch (const std::bad_variant_access &ex)
    {
    }

    try
    {
        std::cout << std::get<std::string>(value) << std::endl;
    }
    catch (const std::bad_variant_access &ex)
    {
    }
}

void printRow(const std::map<Key, Value> &row)
{
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
            while (size == -1 || currentSize < size)
            {
                auto t = std::chrono::system_clock::now();
                long ts = std::chrono::duration_cast<std::chrono::milliseconds>(t.time_since_epoch()).count();
                auto tags = std::vector<std::string>({"A", "B", "C"});
                auto it = generator.randomElement(tags);
                Row row{
                    {"timestamp", ts},
                    {"usage", generator.randomInt(0, 100)},
                    {"host", generator.IPv4()},
                    {"tag", *it++}};
                s.on_next(row);
                std::this_thread::sleep_for(std::chrono::milliseconds(interval));
                currentSize++;
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
    DataTable(const Rows &rows) : data(rows)
    {
    }

    //constructor with one row
    DataTable(const Row &row) : data(std::vector<Row>({row}))
    {
    }

    void addRow(Row && row)
    {
        // sort data here
        data.push_back(std::move(row));
    }

    Rows getData() const
    {
        return data;
    }

    void print() const
    {
        for (auto const &row : data)
        {
            printRow(row);
        }
    }
};

// single window data
class DataWindow : public DataTable
{
private:
    long start;
    long end;

public:
    DataWindow(const Rows &rows, const long start, const long end)
        : DataTable(rows), start(start), end(end)
    {
    }

    DataWindow(const Row &row, const long start, const long end)
        : DataTable(row), start(start), end(end)
    {
    }

    void print() const
    {
        std::cout << "window start - " << start << " end - " << end << std::endl;
        DataTable::print();
    }
};

// multiple window data
class DataWindows
{
private:
    std::map<long, DataWindow> windows;
    long length;
    long firstTimeStamp;

public:
    DataWindows(const long length)
        : length(length), firstTimeStamp(0), windows({})
    {
    }

    void addRow(Row &&row)
    {
        long timestamp = std::get<long>(row.at("timestamp"));

        // Check if there is no first row
        if (firstTimeStamp == 0)
        {
            firstTimeStamp = timestamp;
        }

        // find which window the current row belongs to
        long startTimestamp = firstTimeStamp;
        while (timestamp > (startTimestamp + length))
        {
            startTimestamp += length;
        }

        try
        {
            windows.at(startTimestamp).addRow(std::move(row));
        }
        catch (std::out_of_range error)
        {
            DataWindow newWindw(std::move(row), startTimestamp, startTimestamp + length);
            windows.insert({startTimestamp, newWindw});
        }
    }

    void print() const
    {
        for (auto const &[key, val] : windows)
        {
            std::cout << "key : " << key << std::endl;
            val.print();
        }
    }
};